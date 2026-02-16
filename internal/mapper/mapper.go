package mapper

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/henneberger/metrics-fs/internal/auth"
	"gopkg.in/yaml.v3"
)

type Config struct {
	SourceDir         string
	MapperFileName    string
	InheritParent     bool
	MissingMapperMode string
	DefaultMissingKey string
}

type MappingFile struct {
	Version int           `yaml:"version"`
	Extends string        `yaml:"extends"`
	Rules   []MappingRule `yaml:"rules"`
}

type MappingRule struct {
	Match              RuleMatch  `yaml:"match"`
	Decision           string     `yaml:"decision"`
	ObjectType         string     `yaml:"object_type"`
	Permission         string     `yaml:"permission"`
	MissingResourceKey string     `yaml:"missing_resource_key"`
	Mapper             MapperSpec `yaml:"mapper"`
}

type RuleMatch struct {
	Glob string `yaml:"glob"`
}

type MapperSpec struct {
	Kind              string              `yaml:"kind"`
	Pointer           string              `yaml:"pointer"`
	CanonicalTemplate string              `yaml:"canonical_template"`
	Fields            map[string]string   `yaml:"fields"`
	FromArray         *FromArraySpec      `yaml:"from_array"`
	Emit              []EmitSpec          `yaml:"emit"`
	Normalize         NormalizeSpec       `yaml:"normalize"`
	FallbackPaths     map[string][]string `yaml:"fallback_paths"`
}

type NormalizeSpec struct {
	Lowercase bool `yaml:"lowercase"`
	TrimSlash bool `yaml:"trim_slash"`
}

type FromArraySpec struct {
	Pointer           string            `yaml:"pointer"`
	Fields            map[string]string `yaml:"fields"`
	CanonicalTemplate string            `yaml:"canonical_template"`
}

type EmitSpec struct {
	ObjectType        string            `yaml:"object_type"`
	Permission        string            `yaml:"permission"`
	Fields            map[string]string `yaml:"fields"`
	FromArray         *FromArraySpec    `yaml:"from_array"`
	CanonicalTemplate string            `yaml:"canonical_template"`
}

type SelectedRule struct {
	Decision           string
	MissingResourceKey string
	Rule               MappingRule
	RuleHash           string
}

type Candidate = auth.CandidateKey

func defaults(cfg Config) Config {
	if cfg.MapperFileName == "" {
		cfg.MapperFileName = ".metricfs-map.yaml"
	}
	if cfg.MissingMapperMode == "" {
		cfg.MissingMapperMode = "deny"
	}
	if cfg.DefaultMissingKey == "" {
		cfg.DefaultMissingKey = "deny"
	}
	return cfg
}

func ResolveRuleForFile(filePath string, cfg Config) (*SelectedRule, error) {
	cfg = defaults(cfg)
	absFile, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}
	absSource, err := filepath.Abs(cfg.SourceDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(absFile)
	var mapperPath string
	for {
		candidate := filepath.Join(dir, cfg.MapperFileName)
		if _, err := os.Stat(candidate); err == nil {
			mapperPath = candidate
			break
		}
		if dir == absSource || dir == filepath.Dir(dir) {
			break
		}
		dir = filepath.Dir(dir)
	}
	if mapperPath == "" {
		if cfg.MissingMapperMode == "deny" {
			return nil, fmt.Errorf("no mapper file found for %s", filePath)
		}
		return nil, nil
	}

	rules, ruleHash, err := loadRules(mapperPath, cfg.InheritParent, map[string]bool{})
	if err != nil {
		return nil, err
	}

	for _, r := range rules {
		glob := strings.TrimSpace(r.Match.Glob)
		if glob == "" {
			continue
		}
		relToMapper, err := filepath.Rel(filepath.Dir(mapperPath), absFile)
		if err != nil {
			relToMapper = filepath.Base(absFile)
		}
		relToMapper = filepath.ToSlash(relToMapper)
		name := filepath.Base(absFile)
		m1, _ := doublestar.Match(glob, relToMapper)
		m2, _ := doublestar.Match(glob, name)
		if !m1 && !m2 {
			continue
		}
		decision := r.Decision
		if decision == "" {
			decision = "any"
		}
		if decision != "any" && decision != "all" {
			return nil, fmt.Errorf("invalid decision: %s", decision)
		}
		missing := r.MissingResourceKey
		if missing == "" {
			missing = cfg.DefaultMissingKey
		}
		if missing != "deny" && missing != "ignore" {
			return nil, fmt.Errorf("invalid missing_resource_key: %s", missing)
		}
		return &SelectedRule{Decision: decision, MissingResourceKey: missing, Rule: r, RuleHash: ruleHash}, nil
	}
	if cfg.MissingMapperMode == "deny" {
		return nil, fmt.Errorf("no matching mapper rule for %s", filePath)
	}
	return nil, nil
}

func loadRules(path string, inherit bool, seen map[string]bool) ([]MappingRule, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}
	if seen[abs] {
		return nil, "", fmt.Errorf("extends cycle detected at %s", abs)
	}
	seen[abs] = true

	b, err := os.ReadFile(abs)
	if err != nil {
		return nil, "", err
	}
	var mf MappingFile
	if err := yaml.Unmarshal(b, &mf); err != nil {
		return nil, "", err
	}
	if mf.Version != 1 {
		return nil, "", fmt.Errorf("unsupported mapping version: %d", mf.Version)
	}
	rules := append([]MappingRule{}, mf.Rules...)
	if inherit && strings.TrimSpace(mf.Extends) != "" {
		parent := filepath.Clean(filepath.Join(filepath.Dir(abs), mf.Extends))
		parentRules, _, err := loadRules(parent, inherit, seen)
		if err != nil {
			return nil, "", err
		}
		rules = append(rules, parentRules...)
	}
	canonical, err := canonicalRules(rules)
	if err != nil {
		return nil, "", err
	}
	h := sha1.Sum(canonical)
	return rules, hex.EncodeToString(h[:]), nil
}

func canonicalRules(rules []MappingRule) ([]byte, error) {
	copyRules := make([]MappingRule, len(rules))
	copy(copyRules, rules)
	for i := range copyRules {
		if copyRules[i].Mapper.Fields != nil {
			copyRules[i].Mapper.Fields = sortedMap(copyRules[i].Mapper.Fields)
		}
		if copyRules[i].Mapper.FallbackPaths != nil {
			copyRules[i].Mapper.FallbackPaths = sortedSliceMap(copyRules[i].Mapper.FallbackPaths)
		}
	}
	return json.Marshal(copyRules)
}

func sortedMap(in map[string]string) map[string]string {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := map[string]string{}
	for _, k := range keys {
		out[k] = in[k]
	}
	return out
}

func sortedSliceMap(in map[string][]string) map[string][]string {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := map[string][]string{}
	for _, k := range keys {
		v := append([]string{}, in[k]...)
		out[k] = v
	}
	return out
}

func EvaluateLine(rule *SelectedRule, line []byte) ([]Candidate, error) {
	if rule == nil {
		return nil, errors.New("nil rule")
	}
	var doc any
	if err := json.Unmarshal(line, &doc); err != nil {
		if rule.MissingResourceKey == "deny" {
			return nil, nil
		}
		return nil, nil
	}
	ms := rule.Rule.Mapper
	norm := ms.Normalize
	fallback := ms.FallbackPaths

	buildCandidate := func(objectType, permission, tmpl string, values map[string]any) (Candidate, bool) {
		replaced := tmpl
		for k, v := range values {
			s := fmt.Sprintf("%v", v)
			replaced = strings.ReplaceAll(replaced, "{"+k+"}", s)
		}
		for k := range values {
			_ = k
		}
		for key, ptrs := range fallback {
			needle := "{" + key + "}"
			if strings.Contains(replaced, needle) {
				for _, p := range ptrs {
					if val, ok := resolveRootPointer(doc, p); ok {
						s := strings.TrimSpace(fmt.Sprintf("%v", val))
						if s != "" {
							replaced = strings.ReplaceAll(replaced, needle, s)
							break
						}
					}
				}
			}
		}
		if strings.Contains(replaced, "{") || strings.Contains(replaced, "}") {
			return Candidate{}, false
		}
		id := applyNormalize(replaced, norm)
		prefix := objectType + ":"
		if strings.HasPrefix(id, prefix) {
			id = strings.TrimPrefix(id, prefix)
		}
		if id == "" {
			return Candidate{}, false
		}
		if permission == "" {
			permission = "read"
		}
		return Candidate{ObjectType: objectType, ObjectID: id, Permission: permission}, true
	}

	out := []Candidate{}
	switch ms.Kind {
	case "json_pointer":
		ptr := ms.Pointer
		if !strings.HasPrefix(ptr, "/") {
			return nil, fmt.Errorf("json_pointer pointer must start with /")
		}
		val, ok := resolveRootPointer(doc, ptr)
		if !ok {
			return nil, nil
		}
		cand, ok := buildCandidate(rule.Rule.ObjectType, rule.Rule.Permission, ms.CanonicalTemplate, map[string]any{"value": val})
		if !ok {
			return nil, nil
		}
		out = append(out, cand)
	case "multi_extract":
		for _, e := range ms.Emit {
			if e.FromArray != nil {
				arrV, ok := resolveRootPointer(doc, e.FromArray.Pointer)
				if !ok {
					continue
				}
				arr, ok := arrV.([]any)
				if !ok {
					continue
				}
				for _, item := range arr {
					vals := map[string]any{}
					for k, p := range e.FromArray.Fields {
						if !strings.HasPrefix(p, "./") {
							return nil, fmt.Errorf("from_array field pointer must start with ./")
						}
						v, ok := resolveItemPointer(item, p)
						if !ok {
							break
						}
						vals[k] = v
					}
					cand, ok := buildCandidate(e.ObjectType, e.Permission, e.FromArray.CanonicalTemplate, vals)
					if ok {
						out = append(out, cand)
					}
				}
			} else {
				vals := map[string]any{}
				for k, p := range e.Fields {
					if !strings.HasPrefix(p, "/") {
						return nil, fmt.Errorf("fields pointer must start with /")
					}
					v, ok := resolveRootPointer(doc, p)
					if !ok {
						break
					}
					vals[k] = v
				}
				cand, ok := buildCandidate(e.ObjectType, e.Permission, e.CanonicalTemplate, vals)
				if ok {
					out = append(out, cand)
				}
			}
		}
	default:
		return nil, fmt.Errorf("unsupported mapper kind: %s", ms.Kind)
	}

	uniq := map[Candidate]struct{}{}
	res := make([]Candidate, 0, len(out))
	for _, c := range out {
		if _, ok := uniq[c]; ok {
			continue
		}
		uniq[c] = struct{}{}
		res = append(res, c)
	}
	return res, nil
}

func applyNormalize(s string, n NormalizeSpec) string {
	out := s
	if n.Lowercase {
		out = strings.ToLower(out)
	}
	if n.TrimSlash {
		out = strings.Trim(out, "/")
	}
	return out
}

func resolveRootPointer(doc any, ptr string) (any, bool) {
	if !strings.HasPrefix(ptr, "/") {
		return nil, false
	}
	if ptr == "/" {
		return doc, true
	}
	parts := strings.Split(ptr[1:], "/")
	cur := doc
	for _, raw := range parts {
		tok := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		switch v := cur.(type) {
		case map[string]any:
			next, ok := v[tok]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			idx := -1
			_, err := fmt.Sscanf(tok, "%d", &idx)
			if err != nil || idx < 0 || idx >= len(v) {
				return nil, false
			}
			cur = v[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}

func resolveItemPointer(item any, ptr string) (any, bool) {
	if !strings.HasPrefix(ptr, "./") {
		return nil, false
	}
	if ptr == "./" {
		return item, true
	}
	parts := strings.Split(ptr[2:], "/")
	cur := item
	for _, tok := range parts {
		switch v := cur.(type) {
		case map[string]any:
			next, ok := v[tok]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			idx := -1
			_, err := fmt.Sscanf(tok, "%d", &idx)
			if err != nil || idx < 0 || idx >= len(v) {
				return nil, false
			}
			cur = v[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}
