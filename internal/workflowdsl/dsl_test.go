package workflowdsl_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aryanraj/workflow-orchestrator/internal/workflowdsl"
)

func TestParseYAMLWithMapInput(t *testing.T) {
	// Regression test: yaml.v3 cannot unmarshal a YAML mapping directly into a
	// json.RawMessage field (StepSpec.Input) without the generic-any-then-json-marshal
	// normalization Parse performs — this used to fail with "cannot unmarshal !!map into
	// json.RawMessage" for any step whose `input:` was an object rather than absent/scalar.
	def, err := workflowdsl.Parse([]byte(`
name: with-map-input
version: 1
steps:
  - name: a
    type: http_fetch
    input:
      url: "https://example.com"
      nested:
        x: 1
        y: [1, 2, 3]
`))
	require.NoError(t, err)
	require.NoError(t, def.Validate())
	require.JSONEq(t, `{"url":"https://example.com","nested":{"x":1,"y":[1,2,3]}}`, string(def.Steps[0].Input))
}

func TestParseJSON(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"json-wf","version":1,"steps":[{"name":"a","type":"noop"}]}`))
	require.NoError(t, err)
	require.NoError(t, def.Validate())
	require.Equal(t, "json-wf", def.Name)
}

func TestValidateDefaultsApplied(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"defaults-wf","steps":[{"name":"a","type":"noop"}]}`))
	require.NoError(t, err)
	require.NoError(t, def.Validate())
	s := def.Steps[0]
	require.Equal(t, 1, s.MaxAttempts)
	require.Equal(t, 1000, s.InitialBackoffMS)
	require.Equal(t, 2.0, s.BackoffMultiplier)
	require.Equal(t, 60_000, s.MaxBackoffMS)
	require.Equal(t, 30, s.TimeoutSeconds)
	require.Equal(t, 1, def.Version) // defaulted from 0
}

func TestValidateRejectsUnknownDependency(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"bad-wf","steps":[{"name":"a","type":"noop","depends_on":["ghost"]}]}`))
	require.NoError(t, err)
	err = def.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown step")
}

func TestValidateRejectsCycle(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"cycle-wf","steps":[
		{"name":"a","type":"noop","depends_on":["b"]},
		{"name":"b","type":"noop","depends_on":["a"]}
	]}`))
	require.NoError(t, err)
	err = def.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "cycle")
}

func TestValidateRejectsNoRoot(t *testing.T) {
	// Every step has a dependency, including on itself transitively via a cycle-free but
	// rootless graph — actually simplest rootless case is just every step depending on
	// something, which for a 1-step graph means self-dependency; use a self-dependency here
	// (also independently rejected) is not what we want to isolate, so use two mutually
	// exclusive requirements: build a graph where all nodes have at least one dependency by
	// depending on a step that doesn't exist would trigger the "unknown step" error instead.
	// A clean rootless-but-acyclic graph isn't constructible with depends_on referencing only
	// declared steps without forming a cycle, so instead assert the specific single-step
	// self-dependency case, which the validator also correctly rejects (as a cycle of length
	// 1) before it would ever reach the "no root" check.
	def, err := workflowdsl.Parse([]byte(`{"name":"self-dep","steps":[{"name":"a","type":"noop","depends_on":["a"]}]}`))
	require.NoError(t, err)
	err = def.Validate()
	require.Error(t, err)
}

func TestValidateRejectsDuplicateStepNames(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"dup-wf","steps":[
		{"name":"a","type":"noop"},
		{"name":"a","type":"noop"}
	]}`))
	require.NoError(t, err)
	err = def.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestValidateRejectsEmptySteps(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"empty-wf","steps":[]}`))
	require.NoError(t, err)
	err = def.Validate()
	require.Error(t, err)
}

func TestToNewStepsRoundTrip(t *testing.T) {
	def, err := workflowdsl.Parse([]byte(`{"name":"rt-wf","steps":[
		{"name":"a","type":"noop"},
		{"name":"b","type":"noop","depends_on":["a"]}
	]}`))
	require.NoError(t, err)
	require.NoError(t, def.Validate())
	steps := def.ToNewSteps()
	require.Len(t, steps, 2)
	require.Equal(t, "a", steps[0].StepName)
	require.Equal(t, []string{"a"}, steps[1].DependsOn)
}
