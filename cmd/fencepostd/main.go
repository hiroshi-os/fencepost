package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hiroshi-os/fencepost/internal/reaper"
	"github.com/hiroshi-os/fencepost/internal/scheduler"
	"github.com/hiroshi-os/fencepost/internal/server"
	"github.com/hiroshi-os/fencepost/internal/store/pgstore"
	fencepostv1 "github.com/hiroshi-os/fencepost/proto/fencepost/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
)

func runMigrations(ctx context.Context, connStr string, migrationsDir string) error {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect to db: %w", err)
	}
	defer conn.Close(ctx)

	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".up.sql") {
			path := filepath.Join(migrationsDir, file.Name())
			content, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read migration file %s: %w", file.Name(), err)
			}
			slog.Info("applying migration", "file", file.Name())
			if _, err := conn.Exec(ctx, string(content)); err != nil {
				return fmt.Errorf("exec migration %s: %w", file.Name(), err)
			}
		}
	}
	slog.Info("all migrations applied successfully")
	return nil
}

func main() {
	var (
		grpcAddr      = flag.String("grpc", ":50051", "gRPC listen address")
		httpAddr      = flag.String("http", ":8080", "HTTP health/metrics address")
		dbURL         = flag.String("db", "", "PostgreSQL database connection URL")
		migrationsDir = flag.String("migrations", "migrations", "Path to SQL migrations folder")
		catchupPolicy = flag.String("catchup", "last", "Scheduler catchup policy (none|last|all)")
		reapInterval  = flag.Duration("reap-interval", 1*time.Second, "Reaper interval")
	)
	flag.Parse()

	// Check for 'migrate' command
	args := flag.Args()
	if len(args) > 0 && args[0] == "migrate" {
		connStr := *dbURL
		if connStr == "" {
			connStr = os.Getenv("DATABASE_URL")
		}
		if connStr == "" {
			connStr = "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
		}
		if err := runMigrations(context.Background(), connStr, *migrationsDir); err != nil {
			slog.Error("migration failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if env := os.Getenv("DATABASE_URL"); env != "" && *dbURL == "" {
		*dbURL = env
	}
	if *dbURL == "" {
		*dbURL = "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
	}
	if env := os.Getenv("GRPC_ADDR"); env != "" {
		*grpcAddr = env
	}
	if env := os.Getenv("HTTP_ADDR"); env != "" {
		*httpAddr = env
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pg, err := pgstore.New(ctx, *dbURL)
	if err != nil {
		slog.Error("failed to initialize postgres store", "error", err)
		os.Exit(1)
	}
	defer pg.Close()

	// 1. Start gRPC Queue Server
	grpcServer := grpc.NewServer()
	queueSrv := server.NewQueueServer(pg)
	fencepostv1.RegisterQueueServer(grpcServer, queueSrv)

	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		slog.Error("failed to listen on gRPC addr", "addr", *grpcAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("starting gRPC server", "addr", *grpcAddr)
		if err := grpcServer.Serve(grpcLis); err != nil && err != grpc.ErrServerStopped {
			slog.Error("gRPC server failed", "error", err)
		}
	}()

	// 2. Start HTTP Admin / Metrics Server
	adminSrv := server.NewAdminServer(pg)
	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: adminSrv.Handler(),
	}
	go func() {
		slog.Info("starting HTTP admin/metrics server", "addr", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
		}
	}()

	// 3. Start background Reaper
	rp := reaper.New(pg, *reapInterval, 100)
	go func() {
		if err := rp.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("reaper exited", "error", err)
		}
	}()

	// 4. Start background Scheduler with advisory lock leadership
	sched := scheduler.New(pg.Pool(), pg, scheduler.CatchupPolicy(*catchupPolicy), 1*time.Second)
	go func() {
		if err := sched.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("scheduler exited", "error", err)
		}
	}()

	slog.Info("fencepostd is ready")
	<-ctx.Done()
	slog.Info("shutting down fencepostd...")

	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	slog.Info("fencepostd stopped")
}
