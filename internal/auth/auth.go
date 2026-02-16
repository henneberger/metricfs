package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type CandidateKey struct {
	ObjectType string `json:"object_type"`
	ObjectID   string `json:"object_id"`
	Permission string `json:"permission"`
}

type Authorizer interface {
	IsAllowed(CandidateKey) bool
}

type SetAuthorizer struct {
	allowed map[CandidateKey]struct{}
}

type denyAllAuthorizer struct{}

func (d denyAllAuthorizer) IsAllowed(CandidateKey) bool { return false }

func NewDenyAll() Authorizer { return denyAllAuthorizer{} }

func (a *SetAuthorizer) IsAllowed(c CandidateKey) bool {
	_, ok := a.allowed[c]
	return ok
}

type permissionsDoc struct {
	Allow []CandidateKey `json:"allow"`
}

func NewFromPermissionsFile(path string) (*SetAuthorizer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc permissionsDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	allowed := map[CandidateKey]struct{}{}
	for _, k := range doc.Allow {
		if k.Permission == "" {
			k.Permission = "read"
		}
		allowed[k] = struct{}{}
	}
	return &SetAuthorizer{allowed: allowed}, nil
}

func New(permissionsFile string) (*SetAuthorizer, error) {
	if permissionsFile == "" {
		return nil, fmt.Errorf("--permissions-file is required")
	}
	return NewFromPermissionsFile(permissionsFile)
}

func DebugAllowed(a *SetAuthorizer) []CandidateKey {
	out := make([]CandidateKey, 0, len(a.allowed))
	for k := range a.allowed {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObjectType != out[j].ObjectType {
			return out[i].ObjectType < out[j].ObjectType
		}
		if out[i].ObjectID != out[j].ObjectID {
			return out[i].ObjectID < out[j].ObjectID
		}
		return out[i].Permission < out[j].Permission
	})
	return out
}
