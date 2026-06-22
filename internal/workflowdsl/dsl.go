// Package workflowdsl parses and validates the declarative DAG format workflows are defined
// in (JSON or YAML — YAML is the primary authoring format, JSON is what ends up stored in
// workflow_definitions.dag and is what Parse always normalizes to internally).
package workflowdsl

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

// Definition is the parsed, not-yet-validated form of a workflow DAG document.
type Definition struct {
	Name    string     `json:"name" yaml:"name"`
	Version int        `json:"version" yaml:"version"`
	Steps   []StepSpec `json:"steps" yaml:"steps"`
}

// StepSpec is one DAG node as authored. Zero-valued retry/timeout fields are filled with
// sane defaults by Validate (max_attempts=1 i.e. no retry, timeout_seconds=30).
type StepSpec struct {
	Name              string          `json:"name" yaml:"name"`
	Type              string          `json:"type" yaml:"type"`
	DependsOn         []string        `json:"depends_on" yaml:"depends_on"`
	Input             json.RawMessage `json:"input" yaml:"input"`
	MaxAttempts       int             `json:"max_attempts" yaml:"max_attempts"`
	InitialBackoffMS  int             `json:"initial_backoff_ms" yaml:"initial_backoff_ms"`
	BackoffMultiplier float64         `json:"backoff_multiplier" yaml:"backoff_multiplier"`
	MaxBackoffMS      int             `json:"max_backoff_ms" yaml:"max_backoff_ms"`
	TimeoutSeconds    int             `json:"timeout_seconds" yaml:"timeout_seconds"`
}

// Parse auto-detects JSON vs YAML (JSON documents start with '{' after whitespace) and
// parses accordingly. It does not validate — call Validate() next.
//
// YAML is not unmarshalled directly into Definition: fields like StepSpec.Input are typed as
// json.RawMessage so a validated definition can be stored and later re-served as canonical
// JSON, but yaml.v3 has no notion of "capture this subtree as raw JSON" the way
// encoding/json's json.RawMessage does — it would try to assign a YAML mapping node directly
// to a []byte and fail. So YAML is first decoded into a generic any (yaml.v3 decodes
// mappings as map[string]interface{}, which is directly JSON-marshalable), re-encoded to
// JSON, and then decoded the same way a native JSON document would be.
func Parse(data []byte) (*Definition, error) {
	trimmed := strings.TrimSpace(string(data))
	jsonBytes := data
	if !strings.HasPrefix(trimmed, "{") {
		var generic any
		if err := yaml.Unmarshal(data, &generic); err != nil {
			return nil, fmt.Errorf("workflowdsl: parse yaml: %w", err)
		}
		b, err := json.Marshal(generic)
		if err != nil {
			return nil, fmt.Errorf("workflowdsl: normalize yaml to json: %w", err)
		}
		jsonBytes = b
	}
	var def Definition
	if err := json.Unmarshal(jsonBytes, &def); err != nil {
		return nil, fmt.Errorf("workflowdsl: parse json: %w", err)
	}
	return &def, nil
}

// Validate checks structural correctness (unique names, resolvable dependencies, no cycles,
// at least one root step) and fills in defaults for unset retry/timeout fields. It mutates
// def in place (defaulting) and returns an error describing the first problem found, or nil.
func (d *Definition) Validate() error {
	if strings.TrimSpace(d.Name) == "" {
		return fmt.Errorf("workflowdsl: workflow name is required")
	}
	if d.Version <= 0 {
		d.Version = 1
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("workflowdsl: workflow %q has no steps", d.Name)
	}

	seen := map[string]bool{}
	for i := range d.Steps {
		s := &d.Steps[i]
		if strings.TrimSpace(s.Name) == "" {
			return fmt.Errorf("workflowdsl: step %d has no name", i)
		}
		if seen[s.Name] {
			return fmt.Errorf("workflowdsl: duplicate step name %q", s.Name)
		}
		seen[s.Name] = true
		if strings.TrimSpace(s.Type) == "" {
			return fmt.Errorf("workflowdsl: step %q has no type", s.Name)
		}
		if s.MaxAttempts <= 0 {
			s.MaxAttempts = 1
		}
		if s.InitialBackoffMS <= 0 {
			s.InitialBackoffMS = 1000
		}
		if s.BackoffMultiplier <= 0 {
			s.BackoffMultiplier = 2.0
		}
		if s.MaxBackoffMS <= 0 {
			s.MaxBackoffMS = 60_000
		}
		if s.TimeoutSeconds <= 0 {
			s.TimeoutSeconds = 30
		}
		if len(s.Input) == 0 {
			s.Input = json.RawMessage(`{}`)
		}
	}

	hasRoot := false
	for _, s := range d.Steps {
		for _, dep := range s.DependsOn {
			if !seen[dep] {
				return fmt.Errorf("workflowdsl: step %q depends on unknown step %q", s.Name, dep)
			}
			if dep == s.Name {
				return fmt.Errorf("workflowdsl: step %q depends on itself", s.Name)
			}
		}
		if len(s.DependsOn) == 0 {
			hasRoot = true
		}
	}
	if !hasRoot {
		return fmt.Errorf("workflowdsl: workflow %q has no root step (a step with no depends_on) — every DAG must have an entry point", d.Name)
	}

	if cyc := findCycle(d.Steps); cyc != "" {
		return fmt.Errorf("workflowdsl: dependency cycle detected involving step %q", cyc)
	}
	return nil
}

// findCycle runs a simple DFS with white/gray/black coloring and returns the name of a step
// involved in a cycle, or "" if the graph is acyclic.
func findCycle(steps []StepSpec) string {
	byName := map[string]StepSpec{}
	for _, s := range steps {
		byName[s.Name] = s
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string
	var visit func(name string) string
	visit = func(name string) string {
		color[name] = gray
		stack = append(stack, name)
		for _, dep := range byName[name].DependsOn {
			switch color[dep] {
			case white:
				if r := visit(dep); r != "" {
					return r
				}
			case gray:
				return dep
			}
		}
		color[name] = black
		stack = stack[:len(stack)-1]
		return ""
	}
	for _, s := range steps {
		if color[s.Name] == white {
			if r := visit(s.Name); r != "" {
				return r
			}
		}
	}
	return ""
}

// ToNewSteps converts a validated Definition into store.NewStep rows ready for
// Store.CreateSteps. Callers must call Validate() first.
func (d *Definition) ToNewSteps() []store.NewStep {
	out := make([]store.NewStep, 0, len(d.Steps))
	for _, s := range d.Steps {
		out = append(out, store.NewStep{
			StepName:          s.Name,
			TaskType:          s.Type,
			DependsOn:         append([]string{}, s.DependsOn...),
			Input:             s.Input,
			MaxAttempts:       s.MaxAttempts,
			InitialBackoffMS:  s.InitialBackoffMS,
			BackoffMultiplier: s.BackoffMultiplier,
			MaxBackoffMS:      s.MaxBackoffMS,
			TimeoutSeconds:    s.TimeoutSeconds,
		})
	}
	return out
}

// ToJSON renders the definition as canonical JSON, for storage in workflow_definitions.dag.
func (d *Definition) ToJSON() ([]byte, error) {
	return json.Marshal(d)
}
