// Command loadtest is a small, dependency-free load-test harness for the orchestrator's HTTP
// API. It registers a 12-step workflow definition (a realistic mix of sequential and
// parallel fan-out/fan-in, per the project brief's "10+ steps"), launches runs at a target
// open-loop rate for a fixed duration, then drains — polling every launched run until it
// reaches a terminal status — and prints throughput/latency/error statistics.
//
// Usage:
//
//	go run ./loadtest -http http://localhost:8080 -rate 20 -duration 30s
//
// See loadtest/REPORT.md for a written run against this repo's docker-compose stack and the
// resulting bottleneck analysis.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aryanraj/workflow-orchestrator/internal/config"
)

// apiKey is sent as `Authorization: Bearer <apiKey>` on every request; set from the -key flag
// in main(), defaulting to the same dev key the server falls back to when unconfigured.
var apiKey string

const workflowDefYAML = `
name: loadtest-12step
version: 1
steps:
  - name: intake
    type: noop
  - name: validate
    type: noop
    depends_on: [intake]

  - name: branch_a_1
    type: noop
    depends_on: [validate]
  - name: branch_a_2
    type: noop
    depends_on: [branch_a_1]
  - name: branch_a_3
    type: noop
    depends_on: [branch_a_2]

  - name: branch_b_1
    type: noop
    depends_on: [validate]
  - name: branch_b_2
    type: noop
    depends_on: [branch_b_1]
  - name: branch_b_3
    type: noop
    depends_on: [branch_b_2]

  - name: branch_c_1
    type: noop
    depends_on: [validate]
  - name: branch_c_2
    type: noop
    depends_on: [branch_c_1]

  - name: merge
    type: noop
    depends_on: [branch_a_3, branch_b_3, branch_c_2]
  - name: finalize
    type: noop
    depends_on: [merge]
`

type runResult struct {
	id          string
	launchedAt  time.Time
	launchErr   error
	launchDur   time.Duration
	completedAt time.Time
	finalStatus string
	pollErr     error
}

func main() {
	httpAddr := flag.String("http", "http://localhost:8080", "orchestrator HTTP API base URL")
	rate := flag.Float64("rate", 20, "target workflow launch rate, in workflows/sec (open-loop)")
	duration := flag.Duration("duration", 30*time.Second, "how long to launch new workflows for")
	drainTimeout := flag.Duration("drain-timeout", 60*time.Second, "max time to wait for all launched runs to reach a terminal status")
	pollInterval := flag.Duration("poll-interval", 200*time.Millisecond, "how often each drain worker re-checks a run's status")
	drainConcurrency := flag.Int("drain-concurrency", 32, "number of concurrent goroutines polling run status during drain")
	key := flag.String("key", config.DefaultDevAPIKey, "API key sent as `Authorization: Bearer <key>` (must match the target server's WORKFLOW_API_KEY)")
	flag.Parse()
	apiKey = *key

	client := &http.Client{Timeout: 10 * time.Second}

	fmt.Println("registering load-test workflow definition (12 steps: 2 sequential intro, 3-way fan-out of 3/3/2 steps, fan-in join, finalize)...")
	if err := registerDefinition(client, *httpAddr); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register definition: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("launching workflows at %.1f/s for %s...\n", *rate, *duration)
	results := launchPhase(client, *httpAddr, *rate, *duration)
	fmt.Printf("launch phase complete: %d runs launched (%d failed to launch)\n", len(results), countLaunchErrors(results))

	fmt.Printf("draining: polling up to %d runs concurrently until terminal (timeout %s)...\n", *drainConcurrency, *drainTimeout)
	drainPhase(client, *httpAddr, results, *drainConcurrency, *pollInterval, *drainTimeout)

	printReport(*rate, *duration, results)
}

func registerDefinition(client *http.Client, httpAddr string) error {
	req, err := http.NewRequest(http.MethodPost, httpAddr+"/api/definitions", bytes.NewReader([]byte(workflowDefYAML)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// A 400 "duplicate key" is expected/fine on repeat runs against a persistent database —
	// the definition is already registered from a previous invocation.
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusBadRequest {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func launchPhase(client *http.Client, httpAddr string, rate float64, duration time.Duration) []*runResult {
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.Now().Add(duration)
	var results []*runResult
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Bounds concurrent in-flight launch requests so a slow/overloaded server can't cause
	// unbounded goroutine growth in the harness itself.
	sem := make(chan struct{}, 200)

	for time.Now().Before(deadline) {
		<-ticker.C
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r := launchOne(client, httpAddr)
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return results
}

func launchOne(client *http.Client, httpAddr string) *runResult {
	start := time.Now()
	body, _ := json.Marshal(map[string]any{"name": "loadtest-12step", "input": map[string]any{}})
	req, err := http.NewRequest(http.MethodPost, httpAddr+"/api/runs", bytes.NewReader(body))
	if err != nil {
		return &runResult{launchedAt: start, launchErr: err}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	launchDur := time.Since(start)
	if err != nil {
		return &runResult{launchedAt: start, launchErr: err, launchDur: launchDur}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return &runResult{launchedAt: start, launchErr: fmt.Errorf("status %d: %s", resp.StatusCode, respBody), launchDur: launchDur}
	}
	var created struct {
		ID string
	}
	if err := json.Unmarshal(respBody, &created); err != nil {
		return &runResult{launchedAt: start, launchErr: err, launchDur: launchDur}
	}
	return &runResult{id: created.ID, launchedAt: start, launchDur: launchDur}
}

func drainPhase(client *http.Client, httpAddr string, results []*runResult, concurrency int, pollInterval, timeout time.Duration) {
	var pending []*runResult
	for _, r := range results {
		if r.launchErr == nil {
			pending = append(pending, r)
		}
	}

	work := make(chan *runResult, len(pending))
	for _, r := range pending {
		work <- r
	}
	close(work)

	var wg sync.WaitGroup
	deadline := time.Now().Add(timeout)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				pollUntilTerminal(client, httpAddr, r, pollInterval, deadline)
			}
		}()
	}
	wg.Wait()
}

func pollUntilTerminal(client *http.Client, httpAddr string, r *runResult, pollInterval time.Duration, deadline time.Time) {
	for time.Now().Before(deadline) {
		pollReq, err := http.NewRequest(http.MethodGet, httpAddr+"/api/runs/"+r.id, nil)
		if err != nil {
			r.pollErr = err
			time.Sleep(pollInterval)
			continue
		}
		pollReq.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(pollReq)
		if err != nil {
			r.pollErr = err
			time.Sleep(pollInterval)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var run struct {
			Status string
		}
		if err := json.Unmarshal(body, &run); err != nil {
			r.pollErr = err
			time.Sleep(pollInterval)
			continue
		}
		if run.Status == "COMPLETED" || run.Status == "FAILED" || run.Status == "CANCELLED" {
			r.completedAt = time.Now()
			r.finalStatus = run.Status
			return
		}
		time.Sleep(pollInterval)
	}
	r.finalStatus = "TIMEOUT"
}

func countLaunchErrors(results []*runResult) int {
	n := 0
	for _, r := range results {
		if r.launchErr != nil {
			n++
		}
	}
	return n
}

func printReport(targetRate float64, duration time.Duration, results []*runResult) {
	total := len(results)
	launchErrors := countLaunchErrors(results)
	launched := total - launchErrors

	var completed, failed, cancelled, timedOut, pollErrors int
	var latencies []time.Duration
	var launchDurs []time.Duration

	for _, r := range results {
		if r.launchErr != nil {
			continue
		}
		launchDurs = append(launchDurs, r.launchDur)
		switch r.finalStatus {
		case "COMPLETED":
			completed++
			latencies = append(latencies, r.completedAt.Sub(r.launchedAt))
		case "FAILED":
			failed++
		case "CANCELLED":
			cancelled++
		case "TIMEOUT":
			timedOut++
		}
		if r.pollErr != nil {
			pollErrors++
		}
	}

	achievedRate := float64(launched) / duration.Seconds()

	fmt.Println()
	fmt.Println("========== LOAD TEST REPORT ==========")
	fmt.Printf("target launch rate:      %.1f workflows/sec\n", targetRate)
	fmt.Printf("launch window:           %s\n", duration)
	fmt.Printf("total launch attempts:   %d\n", total)
	fmt.Printf("launch failures:         %d\n", launchErrors)
	fmt.Printf("achieved launch rate:    %.2f workflows/sec\n", achievedRate)
	fmt.Println()
	fmt.Printf("completed:               %d (%.1f%%)\n", completed, pct(completed, launched))
	fmt.Printf("failed (terminal):       %d (%.1f%%)\n", failed, pct(failed, launched))
	fmt.Printf("cancelled:               %d\n", cancelled)
	fmt.Printf("timed out waiting:       %d\n", timedOut)
	fmt.Printf("status-poll errors:      %d\n", pollErrors)
	fmt.Println()
	fmt.Println("run-creation (POST /api/runs) latency:")
	printLatencyStats(launchDurs)
	fmt.Println()
	fmt.Println("end-to-end completion latency (create -> COMPLETED, includes poll-interval granularity):")
	printLatencyStats(latencies)
	fmt.Println("=======================================")
}

func pct(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}

func printLatencyStats(durs []time.Duration) {
	if len(durs) == 0 {
		fmt.Println("  (no samples)")
		return
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	sum := time.Duration(0)
	for _, d := range durs {
		sum += d
	}
	p := func(q float64) time.Duration {
		idx := int(q * float64(len(durs)-1))
		return durs[idx]
	}
	fmt.Printf("  n=%d  min=%s  p50=%s  p90=%s  p99=%s  max=%s  mean=%s\n",
		len(durs), durs[0], p(0.50), p(0.90), p(0.99), durs[len(durs)-1], sum/time.Duration(len(durs)))
}
