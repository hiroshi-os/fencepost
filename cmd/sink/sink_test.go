package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSinkFencingBehavior(t *testing.T) {
	// 1. Fencing ON
	sinkOn, err := NewSinkServer(":memory:", false)
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}

	postEffect := func(s *SinkServer, jobID string, token int64, val string) int {
		body, _ := json.Marshal(EffectRequest{JobID: jobID, Token: token, Value: val})
		req := httptest.NewRequest("POST", "/effects", bytes.NewReader(body))
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		return w.Code
	}

	getStats := func(s *SinkServer) StatsResponse {
		req := httptest.NewRequest("GET", "/stats", nil)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		var st StatsResponse
		_ = json.NewDecoder(w.Body).Decode(&st)
		return st
	}

	// Apply first effect with token 10 -> 200 OK
	if code := postEffect(sinkOn, "job-A", 10, "val-1"); code != http.StatusOK {
		t.Errorf("expected 200 for initial effect, got %d", code)
	}

	// Stale token 9 -> 409 Conflict
	if code := postEffect(sinkOn, "job-A", 9, "val-stale"); code != http.StatusConflict {
		t.Errorf("expected 409 Conflict for stale token, got %d", code)
	}

	// Duplicate attempt with token 12 on already applied job -> 409 Conflict
	if code := postEffect(sinkOn, "job-A", 12, "val-dup"); code != http.StatusConflict {
		t.Errorf("expected 409 Conflict for duplicate attempt, got %d", code)
	}

	stOn := getStats(sinkOn)
	if stOn.DuplicateEffects != 0 {
		t.Errorf("expected 0 duplicate effects with fencing ON, got %d", stOn.DuplicateEffects)
	}
	if stOn.TotalRejected != 2 {
		t.Errorf("expected 2 rejected effects, got %d", stOn.TotalRejected)
	}

	// 2. Negative Control: Fencing OFF
	sinkOff, err := NewSinkServer(":memory:", true)
	if err != nil {
		t.Fatalf("failed to create sink off: %v", err)
	}

	// First call -> 200
	if code := postEffect(sinkOff, "job-B", 10, "val-1"); code != http.StatusOK {
		t.Errorf("expected 200, got %d", code)
	}

	// Stale / duplicate call with fencing OFF -> 200!
	if code := postEffect(sinkOff, "job-B", 9, "val-stale"); code != http.StatusOK {
		t.Errorf("expected 200 with fencing OFF, got %d", code)
	}

	stOff := getStats(sinkOff)
	if stOff.TotalRejected != 0 {
		t.Errorf("expected 0 rejections with fencing OFF, got %d", stOff.TotalRejected)
	}
	if stOff.DuplicateEffects != 1 {
		t.Errorf("expected 1 duplicate effect with fencing OFF, got %d", stOff.DuplicateEffects)
	}
}
