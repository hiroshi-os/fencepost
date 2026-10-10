package server

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/metrics"
	"github.com/hiroshi-os/fencepost/internal/store"
	fencepostv1 "github.com/hiroshi-os/fencepost/proto/fencepost/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type QueueServer struct {
	fencepostv1.UnimplementedQueueServer
	store store.Store
}

func NewQueueServer(st store.Store) *QueueServer {
	return &QueueServer{store: st}
}

func (s *QueueServer) Enqueue(ctx context.Context, req *fencepostv1.EnqueueRequest) (*fencepostv1.EnqueueResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue name required")
	}

	var runAt *time.Time
	if req.RunAt != nil {
		t := req.RunAt.AsTime()
		runAt = &t
	}

	var idemKey *string
	if req.IdempotencyKey != "" {
		k := req.IdempotencyKey
		idemKey = &k
	}

	res, err := s.store.Enqueue(ctx, store.EnqueueParams{
		Queue:          req.Queue,
		Payload:        req.Payload,
		RunAt:          runAt,
		Priority:       req.Priority,
		IdempotencyKey: idemKey,
		MaxAttempts:    req.MaxAttempts,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue failed: %v", err)
	}

	return &fencepostv1.EnqueueResponse{
		JobId:        res.JobID.String(),
		Deduplicated: res.Deduplicated,
	}, nil
}

func (s *QueueServer) Lease(ctx context.Context, req *fencepostv1.LeaseRequest) (*fencepostv1.LeaseResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue name required")
	}
	workerID := req.WorkerId
	if workerID == "" {
		workerID = "worker-" + uuid.NewString()[:8]
	}
	maxJobs := req.MaxJobs
	if maxJobs <= 0 {
		maxJobs = 1
	}

	ttl := 30 * time.Second
	if req.LeaseTtl != nil && req.LeaseTtl.AsDuration() > 0 {
		ttl = req.LeaseTtl.AsDuration()
	}

	waitDur := time.Duration(0)
	if req.Wait != nil && req.Wait.AsDuration() > 0 {
		waitDur = req.Wait.AsDuration()
	}

	deadline := time.Now().Add(waitDur)
	for {
		leased, err := s.store.Lease(ctx, req.Queue, workerID, maxJobs, ttl)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "lease error: %v", err)
		}

		if len(leased) > 0 || time.Now().After(deadline) || waitDur == 0 {
			protoJobs := make([]*fencepostv1.LeasedJob, len(leased))
			for i, j := range leased {
				protoJobs[i] = &fencepostv1.LeasedJob{
					Id:             j.ID.String(),
					Payload:        j.Payload,
					Attempt:        j.Attempt,
					FencingToken:   j.FencingToken,
					LeaseExpiresAt: timestamppb.New(j.LeaseExpiresAt),
				}
			}
			return &fencepostv1.LeaseResponse{Jobs: protoJobs}, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *QueueServer) Heartbeat(ctx context.Context, req *fencepostv1.HeartbeatRequest) (*fencepostv1.HeartbeatResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	extend := 30 * time.Second
	if req.Extend != nil && req.Extend.AsDuration() > 0 {
		extend = req.Extend.AsDuration()
	}

	newExpiry, err := s.store.Heartbeat(ctx, jobID, req.FencingToken, extend)
	if err != nil {
		if errors.Is(err, store.ErrStaleToken) {
			metrics.StaleTokenRejectionsTotal.Inc()
			return nil, status.Error(codes.FailedPrecondition, "stale fencing token or lease expired")
		}
		return nil, status.Errorf(codes.Internal, "heartbeat failed: %v", err)
	}

	return &fencepostv1.HeartbeatResponse{
		LeaseExpiresAt: timestamppb.New(newExpiry),
	}, nil
}

func (s *QueueServer) Complete(ctx context.Context, req *fencepostv1.CompleteRequest) (*fencepostv1.CompleteResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	err = s.store.Complete(ctx, jobID, req.FencingToken, req.Result)
	if err != nil {
		if errors.Is(err, store.ErrStaleToken) {
			metrics.StaleTokenRejectionsTotal.Inc()
			return nil, status.Error(codes.FailedPrecondition, "stale fencing token")
		}
		return nil, status.Errorf(codes.Internal, "complete failed: %v", err)
	}

	return &fencepostv1.CompleteResponse{Ok: true}, nil
}

func (s *QueueServer) Fail(ctx context.Context, req *fencepostv1.FailRequest) (*fencepostv1.FailResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	err = s.store.Fail(ctx, jobID, req.FencingToken, req.Error, req.Retryable)
	if err != nil {
		if errors.Is(err, store.ErrStaleToken) {
			metrics.StaleTokenRejectionsTotal.Inc()
			return nil, status.Error(codes.FailedPrecondition, "stale fencing token")
		}
		return nil, status.Errorf(codes.Internal, "fail failed: %v", err)
	}

	return &fencepostv1.FailResponse{Ok: true}, nil
}

func (s *QueueServer) Cancel(ctx context.Context, req *fencepostv1.CancelRequest) (*fencepostv1.CancelResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	err = s.store.Cancel(ctx, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cancel failed: %v", err)
	}

	return &fencepostv1.CancelResponse{Ok: true}, nil
}

func (s *QueueServer) Get(ctx context.Context, req *fencepostv1.GetRequest) (*fencepostv1.GetResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	j, err := s.store.Get(ctx, jobID)
	if err != nil {
		if errors.Is(err, store.ErrJobNotFound) {
			return nil, status.Error(codes.NotFound, "job not found")
		}
		return nil, status.Errorf(codes.Internal, "get job failed: %v", err)
	}

	var leaseOwner string
	if j.LeaseOwner != nil {
		leaseOwner = *j.LeaseOwner
	}
	var expiresProto *timestamppb.Timestamp
	if j.LeaseExpiresAt != nil {
		expiresProto = timestamppb.New(*j.LeaseExpiresAt)
	}
	var tokenVal int64
	if j.FencingToken != nil {
		tokenVal = *j.FencingToken
	}
	var idemKey string
	if j.IdempotencyKey != nil {
		idemKey = *j.IdempotencyKey
	}
	var lastErr string
	if j.LastError != nil {
		lastErr = *j.LastError
	}

	return &fencepostv1.GetResponse{
		Job: &fencepostv1.Job{
			Id:             j.ID.String(),
			Queue:          j.Queue,
			Payload:        j.Payload,
			Priority:       j.Priority,
			RunAt:          timestamppb.New(j.RunAt),
			State:          string(j.State),
			Attempts:       j.Attempts,
			MaxAttempts:    j.MaxAttempts,
			LeaseOwner:     leaseOwner,
			LeaseExpiresAt: expiresProto,
			FencingToken:   tokenVal,
			IdempotencyKey: idemKey,
			Result:         j.Result,
			LastError:      lastErr,
			CreatedAt:      timestamppb.New(j.CreatedAt),
			UpdatedAt:      timestamppb.New(j.UpdatedAt),
		},
	}, nil
}

var _ fencepostv1.QueueServer = (*QueueServer)(nil)
var _ = durationpb.New
