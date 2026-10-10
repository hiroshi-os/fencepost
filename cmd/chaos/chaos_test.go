package main

import (
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestSuspendResume(t *testing.T) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("powershell", "-Command", "Start-Sleep -Seconds 10")
	} else {
		cmd = exec.Command("sleep", "10")
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sleep: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	pid := cmd.Process.Pid
	if err := suspendProcess(pid); err != nil {
		t.Fatalf("suspendProcess failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if err := resumeProcess(pid); err != nil {
		t.Fatalf("resumeProcess failed: %v", err)
	}
}
