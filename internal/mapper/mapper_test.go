package mapper

import (
	"testing"
)

func TestResolveRuleForOrders(t *testing.T) {
	r, err := ResolveRuleForFile("../../examples/metrics/orders.jsonl", Config{
		SourceDir:         "../../examples/metrics",
		MapperFileName:    ".metricfs-map.yaml",
		InheritParent:     true,
		MissingMapperMode: "deny",
		DefaultMissingKey: "deny",
	})
	if err != nil {
		t.Fatalf("resolve rule: %v", err)
	}
	if r.Rule.Mapper.Kind != "json_pointer" {
		t.Fatalf("expected json_pointer, got %s", r.Rule.Mapper.Kind)
	}
}

func TestEvaluateOpenLineageMultiExtract(t *testing.T) {
	r, err := ResolveRuleForFile("../../examples/metrics/openlineage/event.jsonl", Config{
		SourceDir:         "../../examples/metrics",
		MapperFileName:    ".metricfs-map.yaml",
		InheritParent:     true,
		MissingMapperMode: "deny",
		DefaultMissingKey: "deny",
	})
	if err != nil {
		t.Fatalf("resolve rule: %v", err)
	}
	line := []byte(`{"event":{"inputs":[{"namespace":"prod/snowflake","name":"sales/orders"}],"outputs":[{"namespace":"prod/snowflake","name":"sales/orders_enriched"}],"job":{"namespace":"prod/airflow","name":"daily_etl"},"run":{"runId":"run_20260216"}}}`)
	cands, err := EvaluateLine(r, line)
	if err != nil {
		t.Fatalf("evaluate line: %v", err)
	}
	if len(cands) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(cands))
	}
	if cands[0].ObjectType != "job" || cands[0].ObjectID != "prod/airflow/daily_etl" {
		t.Fatalf("unexpected candidate: %#v", cands[0])
	}
}

func TestEvaluateOpenLineageFallbackFromFacets(t *testing.T) {
	r, err := ResolveRuleForFile("../../examples/metrics/openlineage/events.jsonl", Config{
		SourceDir:         "../../examples/metrics",
		MapperFileName:    ".metricfs-map.yaml",
		InheritParent:     true,
		MissingMapperMode: "deny",
		DefaultMissingKey: "deny",
	})
	if err != nil {
		t.Fatalf("resolve rule: %v", err)
	}
	line := []byte(`{"event":{"inputs":[],"outputs":[],"job":{},"run":{},"facets":{"job":{"namespace":"prod/airflow","name":"daily_etl"},"run":{"runId":"run_20260216"}}}}`)
	cands, err := EvaluateLine(r, line)
	if err != nil {
		t.Fatalf("evaluate line: %v", err)
	}
	if len(cands) == 0 {
		t.Fatalf("expected fallback candidates, got none")
	}
	foundJob := false
	for _, c := range cands {
		if c.ObjectType == "job" && c.ObjectID == "prod/airflow/daily_etl" {
			foundJob = true
		}
	}
	if !foundJob {
		t.Fatalf("expected job fallback candidate, got %#v", cands)
	}
}
