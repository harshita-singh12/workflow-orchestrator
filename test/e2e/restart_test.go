// Package e2e contains true end-to-end tests that exercise the system as a set of real OS
// processes talking over real Postgres/Redis/gRPC/HTTP — not in-process fakes. This is
// deliberate: the whole point of TestServerRestartMidWorkflow is to prove durable execution
// survives an actual `kill -9` of the server binary, which is meaningless to test against an
// in-process engine.Engine value that can't crash independently of the test itself.
//
// Requires a reachable Postgres and Redis (see README.md "Running the durability test").
// Defaults match docker-compose.yml's exposed ports; override with E2E_DATABASE_URL /
// E2E_REDIS_ADDR. The test skips (not fails) if neither is reachable, so `go test ./...`
// stays green in environments without Docker running.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerpb "github.com/aryanraj/workflow-orchestrator/gen/workerpb"
	"github.com/aryanraj/workflow-orchestrator/internal/queue/redisstream"
	"github.com/aryanraj/workflow-orchestrator/internal/worker"
)

func e2eDatabaseURL() string {
	if v := os.Getenv("E2E_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://orchestrator:orchestrator@localhost:5433/orchestrator?sslmode=disable"
}

func e2eRedisAddr() string {
	if v := os.Getenv("E2E_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6380"
}

func requireInfra(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, e2eDatabaseURL())
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("skipping e2e test: postgres not reachable at %s (start it with `docker compose up -d postgres redis` or see README): %v", e2eDatabaseURL(), err)
	}
	pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: e2eRedisAddr()})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping e2e test: redis not reachable at %s: %v", e2eRedisAddr(), err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// buildServerBinary compiles cmd/server once into a temp directory and returns its path.
func buildServerBinary(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "wf-server-e2e")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/server")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build server binary: %v\n%s", err, out)
	}
	return bin
}

type serverProcess struct {
	cmd      *exec.Cmd
	httpAddr string
	logPath  string
}

func startServer(t *testing.T, bin string, httpPort, grpcPort int, nodeID string, dbURL, redisAddr string, logPath string) *serverProcess {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dbURL,
		"REDIS_ADDR="+redisAddr,
		"HTTP_ADDR=:"+strconv.Itoa(httpPort),
		"GRPC_ADDR=:"+strconv.Itoa(grpcPort),
		"NODE_ID="+nodeID,
		"ENABLE_SHARDING=false",
		// Short lease so the "crash while a task is LEASED and never reported" path (the
		// hardest case: work happened but the report never landed) resolves quickly via the
		// reaper instead of requiring the test to wait out a 30s production lease.
		"LEASE_DURATION_SECONDS=3",
	)
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	sp := &serverProcess{cmd: cmd, httpAddr: fmt.Sprintf("http://127.0.0.1:%d", httpPort), logPath: logPath}
	waitHealthy(t, sp.httpAddr, 10*time.Second)
	return sp
}

func waitHealthy(t *testing.T, httpAddr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(httpAddr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server at %s never became healthy", httpAddr)
}

// killDashNine sends SIGKILL to the server process — equivalent to `kill -9 <pid>` — and
// waits for the OS to reap it, simulating an abrupt crash with zero graceful shutdown.
func killDashNine(t *testing.T, sp *serverProcess) {
	t.Helper()
	if err := sp.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill -9 server: %v", err)
	}
	_ = sp.cmd.Wait() // reap; a non-zero/signal-killed exit status is expected here
}

// workflowYAML is parameterized by name so repeated test runs against the same (persistent,
// shared) Postgres container never collide on workflow_definitions' (name, version) unique
// constraint.
func workflowYAML(name string) string {
	return fmt.Sprintf(`
name: %s
version: 1
steps:
  - name: step1
    type: counted_step
    depends_on: []
    max_attempts: 3
    initial_backoff_ms: 200
  - name: step2
    type: counted_step
    depends_on: [step1]
    max_attempts: 3
    initial_backoff_ms: 200
  - name: step3
    type: counted_step
    depends_on: [step2]
    max_attempts: 3
    initial_backoff_ms: 200
  - name: step4
    type: counted_step
    depends_on: [step3]
    max_attempts: 3
    initial_backoff_ms: 200
  - name: step5
    type: counted_step
    depends_on: [step4]
    max_attempts: 3
    initial_backoff_ms: 200
  - name: step6
    type: counted_step
    depends_on: [step5]
    max_attempts: 3
    initial_backoff_ms: 200
`, name)
}

type runStatusResp struct {
	Status string          `json:"Status"`
	Error  *string         `json:"Error"`
	Output json.RawMessage `json:"Output"`
}

// historyEvent mirrors store.HistoryEvent's JSON shape. store types carry no `json:` tags,
// so encoding/json uses the exported Go field names verbatim on the wire (e.g. "EventType",
// not "event_type") — match that here rather than tagging with snake_case.
type historyEvent struct {
	EventType string
	Payload   json.RawMessage
}

// TestServerRestartMidWorkflow is the durability test required by the project spec: it
// starts the real server binary as a subprocess, kicks off a 6-step sequential workflow,
// waits for genuine partial progress, SIGKILLs the server mid-flight, restarts it against
// the same Postgres/Redis, and asserts the workflow still completes correctly — proving
// state lives in the database, not in server memory.
func TestServerRestartMidWorkflow(t *testing.T) {
	requireInfra(t)

	bin := buildServerBinary(t)
	httpPort := freePort(t)
	grpcPort := freePort(t)
	dbURL := e2eDatabaseURL()
	redisAddr := e2eRedisAddr()
	tmpDir := t.TempDir()

	ctx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()

	// --- phase 1: start server, register workflow, start a run ---
	sp1 := startServer(t, bin, httpPort, grpcPort, "e2e-node-1", dbURL, redisAddr, filepath.Join(tmpDir, "server1.log"))
	// Belt-and-braces cleanup: killDashNine() below already kills sp1 on the happy path, but
	// if the test fails/fatals earlier than that (e.g. workflow never makes partial progress
	// within the timeout), nothing else would ever reap this process — Kill()/Wait() on an
	// already-dead process is a harmless no-op, so it's safe to always defer this too.
	defer func() {
		_ = sp1.cmd.Process.Kill()
		_ = sp1.cmd.Wait()
	}()

	// The worker is an in-process goroutine using the real gRPC client + real Redis Streams
	// consumer — only the *server* is the external process under test, per the spec ("kill
	// -9 the server process ... restart it"). It's started only after the server is already
	// listening so we're testing the crash-recovery path itself, not an unrelated startup
	// race. The worker deliberately stays alive across the server's downtime window: some of
	// its ReportResult calls will fail while the server is down, which is exactly the
	// scenario that exercises the lease-expiry reaper's retry path on top of the plain
	// "resume dispatch of not-yet-started steps" path.
	var counter atomic.Int64
	var mu sync.Mutex
	seenAttempts := map[string]bool{}

	grpcConn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	defer grpcConn.Close()
	client := workerpb.NewWorkerServiceClient(grpcConn)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	q := redisstream.New(rdb, nil)
	w := worker.New("e2e-worker", q, client, nil)
	// Reclaim unacked messages quickly (default is 15s) so a transient claim RPC failure —
	// e.g. one that lands in the exact instant the server goes down — doesn't stall the test;
	// production deployments would use a longer window to avoid competing with a merely-slow
	// (not dead) worker.
	w.IdleTimeout = 2 * time.Second
	w.Register("counted_step", func(ctx context.Context, input json.RawMessage, idemKey string) (json.RawMessage, error) {
		mu.Lock()
		firstTime := !seenAttempts[idemKey]
		seenAttempts[idemKey] = true
		mu.Unlock()
		if firstTime {
			counter.Add(1)
		}
		time.Sleep(200 * time.Millisecond)
		return json.RawMessage(`{"ok":true}`), nil
	})
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			t.Logf("worker exited: %v", err)
		}
	}()

	httpClient := &http.Client{Timeout: 5 * time.Second}
	wfName := fmt.Sprintf("e2e-restart-workflow-%d", time.Now().UnixNano())
	postJSON(t, httpClient, sp1.httpAddr+"/api/definitions", []byte(workflowYAML(wfName)), "application/yaml")

	runID := postJSON(t, httpClient, sp1.httpAddr+"/api/runs", []byte(fmt.Sprintf(`{"name":%q,"input":{}}`, wfName)), "application/json")
	var created map[string]any
	if err := json.Unmarshal(runID, &created); err != nil {
		t.Fatalf("decode create-run response: %v", err)
	}
	id, _ := created["ID"].(string)
	if id == "" {
		t.Fatalf("no run id in response: %s", runID)
	}
	t.Logf("started run %s", id)

	// Wait for genuine partial progress: at least 2 of 6 steps completed, but not all 6 —
	// otherwise "kill mid-workflow" wouldn't actually be mid-workflow.
	waitForCondition(t, 20*time.Second, func() bool {
		hist := getHistory(t, httpClient, sp1.httpAddr, id)
		completed := countStepCompleted(hist)
		return completed >= 2 && completed < 6
	})
	preKillHist := getHistory(t, httpClient, sp1.httpAddr, id)
	preKillCompleted := countStepCompleted(preKillHist)
	t.Logf("pre-kill: %d/6 steps completed", preKillCompleted)
	if preKillCompleted >= 6 {
		t.Fatalf("workflow completed before we could kill the server; test is not exercising mid-workflow crash")
	}

	// --- phase 2: kill -9 the server mid-workflow ---
	killDashNine(t, sp1)
	t.Log("server killed with SIGKILL")

	// Give the worker a moment to notice the server is gone / any in-flight RPC to fail, so
	// we're genuinely testing recovery rather than a lucky race where nothing was in flight.
	time.Sleep(300 * time.Millisecond)

	// --- phase 3: restart the server against the same database ---
	sp2 := startServer(t, bin, httpPort, grpcPort, "e2e-node-2", dbURL, redisAddr, filepath.Join(tmpDir, "server2.log"))
	defer func() {
		_ = sp2.cmd.Process.Kill()
		_ = sp2.cmd.Wait()
	}()
	t.Log("server restarted")

	// --- phase 4: assert the workflow completes correctly ---
	waitForCondition(t, 30*time.Second, func() bool {
		run := getRun(t, httpClient, sp1.httpAddr, id)
		return run.Status == "COMPLETED" || run.Status == "FAILED"
	})

	run := getRun(t, httpClient, sp1.httpAddr, id)
	if run.Status != "COMPLETED" {
		errMsg := ""
		if run.Error != nil {
			errMsg = *run.Error
		}
		t.Fatalf("workflow did not complete after restart: status=%s error=%s\nserver1 log:\n%s\nserver2 log:\n%s",
			run.Status, errMsg, readFile(sp1.logPath), readFile(sp2.logPath))
	}

	steps := getSteps(t, httpClient, sp1.httpAddr, id)
	if len(steps) != 6 {
		t.Fatalf("expected 6 steps, got %d", len(steps))
	}
	for _, s := range steps {
		if s.Status != "COMPLETED" {
			t.Errorf("step %s has status %s, want COMPLETED", s.StepName, s.Status)
		}
	}

	if got := counter.Load(); got < 6 {
		t.Errorf("expected the counted_step handler to have run at least 6 times (once per step), got %d", got)
	} else {
		t.Logf("counted_step handler ran %d times across 6 steps (>6 is expected/documented when a crash lands between a handler finishing and its result being reported — see the durability-test section of README.md)", got)
	}

	t.Logf("PASS: workflow survived a SIGKILL of the server process mid-flight and completed correctly after restart")
}

func postJSON(t *testing.T, client *http.Client, url string, body []byte, contentType string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	respBody := readBody(t, resp)
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s returned %d: %s", url, resp.StatusCode, respBody)
	}
	return respBody
}

func getRun(t *testing.T, client *http.Client, httpAddr, id string) runStatusResp {
	t.Helper()
	resp, err := client.Get(httpAddr + "/api/runs/" + id)
	if err != nil {
		t.Fatalf("GET run: %v", err)
	}
	defer resp.Body.Close()
	body := readBody(t, resp)
	var out runStatusResp
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode run: %v (%s)", err, body)
	}
	return out
}

type stepResp struct {
	StepName string `json:"StepName"`
	Status   string `json:"Status"`
}

func getSteps(t *testing.T, client *http.Client, httpAddr, id string) []stepResp {
	t.Helper()
	resp, err := client.Get(httpAddr + "/api/runs/" + id + "/steps")
	if err != nil {
		t.Fatalf("GET steps: %v", err)
	}
	defer resp.Body.Close()
	body := readBody(t, resp)
	var out []stepResp
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode steps: %v (%s)", err, body)
	}
	return out
}

func getHistory(t *testing.T, client *http.Client, httpAddr, id string) []historyEvent {
	t.Helper()
	resp, err := client.Get(httpAddr + "/api/runs/" + id + "/history")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body := readBody(t, resp)
	var out []historyEvent
	_ = json.Unmarshal(body, &out)
	return out
}

func countStepCompleted(hist []historyEvent) int {
	n := 0
	for _, h := range hist {
		if h.EventType == "step_completed" {
			n++
		}
	}
	return n
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return b
}

func readFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(could not read log: %v)", err)
	}
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}
