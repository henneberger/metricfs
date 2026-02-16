package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type SpiceDBConfig struct {
	Endpoint    string
	Token       string
	Subject     string
	Consistency string
}

type SpiceDBAuthorizer struct {
	client *http.Client
	url    string
	token  string

	subject     subjectRef
	consistency map[string]any

	mu    sync.RWMutex
	cache map[CandidateKey]bool
}

func NewSpiceDB(cfg SpiceDBConfig) (*SpiceDBAuthorizer, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("spicedb endpoint is required")
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid spicedb endpoint %q: %w", cfg.Endpoint, err)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("invalid spicedb endpoint %q", cfg.Endpoint)
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, fmt.Errorf("spicedb token is required")
	}
	subject, err := parseSubject(cfg.Subject)
	if err != nil {
		return nil, err
	}
	consistency, err := parseConsistency(cfg.Consistency)
	if err != nil {
		return nil, err
	}
	return &SpiceDBAuthorizer{
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		url:         strings.TrimRight(endpoint, "/") + "/v1/permissions/check",
		token:       cfg.Token,
		subject:     subject,
		consistency: consistency,
		cache:       map[CandidateKey]bool{},
	}, nil
}

func (a *SpiceDBAuthorizer) Close() error {
	return nil
}

func (a *SpiceDBAuthorizer) IsAllowed(c CandidateKey) bool {
	if c.Permission == "" {
		c.Permission = "read"
	}
	if c.ObjectType == "" || c.ObjectID == "" {
		return false
	}
	a.mu.RLock()
	allowed, ok := a.cache[c]
	a.mu.RUnlock()
	if ok {
		return allowed
	}

	allowed, err := a.checkRemote(c)
	if err != nil {
		return false
	}
	a.mu.Lock()
	a.cache[c] = allowed
	a.mu.Unlock()
	return allowed
}

type objectRef struct {
	ObjectType string `json:"objectType"`
	ObjectID   string `json:"objectId"`
}

type subjectRef struct {
	Object           objectRef `json:"object"`
	OptionalRelation string    `json:"optionalRelation,omitempty"`
}

type checkPermissionRequest struct {
	Consistency map[string]any `json:"consistency,omitempty"`
	Resource    objectRef      `json:"resource"`
	Permission  string         `json:"permission"`
	Subject     subjectRef     `json:"subject"`
}

type checkPermissionResponse struct {
	Permissionship string `json:"permissionship"`
}

func (a *SpiceDBAuthorizer) checkRemote(c CandidateKey) (bool, error) {
	body := checkPermissionRequest{
		Consistency: a.consistency,
		Resource: objectRef{
			ObjectType: c.ObjectType,
			ObjectID:   c.ObjectID,
		},
		Permission: c.Permission,
		Subject:    a.subject,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, a.url, bytes.NewReader(b))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.token)
	resp, err := a.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("spicedb check failed: %s", resp.Status)
	}
	var out checkPermissionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}
	return out.Permissionship == "PERMISSIONSHIP_HAS_PERMISSION", nil
}

func parseSubject(raw string) (subjectRef, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return subjectRef{}, fmt.Errorf("spicedb subject is required")
	}
	relation := ""
	if i := strings.Index(s, "#"); i >= 0 {
		relation = strings.TrimSpace(s[i+1:])
		s = strings.TrimSpace(s[:i])
		if relation == "" {
			return subjectRef{}, fmt.Errorf("invalid subject relation in %q", raw)
		}
	}
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return subjectRef{}, fmt.Errorf("invalid subject %q, expected type:id or type:id#relation", raw)
	}
	return subjectRef{
		Object: objectRef{
			ObjectType: strings.TrimSpace(parts[0]),
			ObjectID:   strings.TrimSpace(parts[1]),
		},
		OptionalRelation: relation,
	}, nil
}

func parseConsistency(raw string) (map[string]any, error) {
	mode := strings.TrimSpace(strings.ToLower(raw))
	if mode == "" {
		mode = "minimize_latency"
	}
	switch mode {
	case "minimize_latency":
		return map[string]any{"minimizeLatency": true}, nil
	case "fully_consistent":
		return map[string]any{"fullyConsistent": true}, nil
	default:
		return nil, fmt.Errorf("unsupported spicedb consistency mode %q", raw)
	}
}
