package server_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/server"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/hiroshi-os/fencepost/internal/store/memstore"
	fencepostv1 "github.com/hiroshi-os/fencepost/proto/fencepost/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

func setupTestGRPC(t *testing.T) (fencepostv1.QueueClient, *memstore.MemStore, func()) {
	mem := memstore.New(time.Now())
	srv := server.NewQueueServer(mem)

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	fencepostv1.RegisterQueueServer(s, srv)

	go func() {
		_ = s.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	client := fencepostv1.NewQueueClient(conn)
	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}

	return client, mem, cleanup
}

func TestGRPCQueueLifecycle(t *testing.T) {
	client, _, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue
	enqResp, err := client.Enqueue(ctx, &fencepostv1.EnqueueRequest{
		Queue:          "test-q",
		Payload:        []byte("payload-1"),
		Priority:       5,
		IdempotencyKey: "idem-1",
		MaxAttempts:    3,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if enqResp.Deduplicated {
		t.Errorf("expected not deduplicated")
	}

	// Lease
	leaseResp, err := client.Lease(ctx, &fencepostv1.LeaseRequest{
		Queue:    "test-q",
		WorkerId: "worker-1",
		MaxJobs:  1,
		LeaseTtl: durationpb.New(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	if len(leaseResp.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(leaseResp.Jobs))
	}
	leased := leaseResp.Jobs[0]
	if leased.Id != enqResp.JobId {
		t.Fatalf("expected job id %s, got %s", enqResp.JobId, leased.Id)
	}

	// Heartbeat with stale token
	_, err = client.Heartbeat(ctx, &fencepostv1.HeartbeatRequest{
		JobId:        leased.Id,
		FencingToken: leased.FencingToken + 999,
		Extend:       durationpb.New(5 * time.Second),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for stale heartbeat, got %v", err)
	}

	// Valid Heartbeat
	hbResp, err := client.Heartbeat(ctx, &fencepostv1.HeartbeatRequest{
		JobId:        leased.Id,
		FencingToken: leased.FencingToken,
		Extend:       durationpb.New(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("valid heartbeat failed: %v", err)
	}
	if hbResp.LeaseExpiresAt == nil {
		t.Fatalf("expected new lease expiry")
	}

	// Complete with stale token
	_, err = client.Complete(ctx, &fencepostv1.CompleteRequest{
		JobId:        leased.Id,
		FencingToken: leased.FencingToken + 999,
		Result:       []byte("ok"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for stale complete, got %v", err)
	}

	// Complete with valid token
	compResp, err := client.Complete(ctx, &fencepostv1.CompleteRequest{
		JobId:        leased.Id,
		FencingToken: leased.FencingToken,
		Result:       []byte("success-result"),
	})
	if err != nil || !compResp.Ok {
		t.Fatalf("valid complete failed: %v", err)
	}

	// Get
	getResp, err := client.Get(ctx, &fencepostv1.GetRequest{JobId: leased.Id})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if getResp.Job.State != "succeeded" {
		t.Fatalf("expected state succeeded, got %s", getResp.Job.State)
	}
}

func TestAdminHTTPServer(t *testing.T) {
	mem := memstore.New(time.Now())
	admin := server.NewAdminServer(mem)

	// Test /healthz
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	admin.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /healthz, got %d", w.Code)
	}

	// Test /metrics
	reqM := httptest.NewRequest("GET", "/metrics", nil)
	wM := httptest.NewRecorder()
	admin.Handler().ServeHTTP(wM, reqM)
	if wM.Code != http.StatusOK {
		t.Errorf("expected 200 for /metrics, got %d", wM.Code)
	}

	// Test /admin/jobs/{id}
	ctx := context.Background()
	enq, _ := mem.Enqueue(ctx, store.EnqueueParams{
		Queue:   "admin-q",
		Payload: []byte("payload"),
	})

	reqT := httptest.NewRequest("GET", "/admin/jobs/"+enq.JobID.String(), nil)
	wT := httptest.NewRecorder()
	admin.Handler().ServeHTTP(wT, reqT)
	if wT.Code != http.StatusOK {
		t.Fatalf("expected 200 for timeline, got %d", wT.Code)
	}

	var parsed map[string]any
	if err := json.Unmarshal(wT.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse timeline JSON: %v", err)
	}
	if parsed["job"] == nil || parsed["events"] == nil {
		t.Errorf("expected job and events in timeline response: %v", parsed)
	}

	// Unknown job id
	req404 := httptest.NewRequest("GET", "/admin/jobs/"+uuid.NewString(), nil)
	w404 := httptest.NewRecorder()
	admin.Handler().ServeHTTP(w404, req404)
	if w404.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown job, got %d", w404.Code)
	}
}
