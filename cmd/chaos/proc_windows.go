//go:build windows

package main

import (
	"fmt"
	"syscall"
)

var (
	modntdll             = syscall.NewLazyDLL("ntdll.dll")
	procNtSuspendProcess = modntdll.NewProc("NtSuspendProcess")
	procNtResumeProcess  = modntdll.NewProc("NtResumeProcess")
)

const processSuspendResume = 0x0800

func suspendProcess(pid int) error {
	handle, err := syscall.OpenProcess(processSuspendResume, false, uint32(pid))
	if err != nil {
		return fmt.Errorf("open process: %w", err)
	}
	defer syscall.CloseHandle(handle)

	r, _, _ := procNtSuspendProcess.Call(uintptr(handle))
	if r != 0 {
		return fmt.Errorf("NtSuspendProcess status: %x", r)
	}
	return nil
}

func resumeProcess(pid int) error {
	handle, err := syscall.OpenProcess(processSuspendResume, false, uint32(pid))
	if err != nil {
		return fmt.Errorf("open process: %w", err)
	}
	defer syscall.CloseHandle(handle)

	r, _, _ := procNtResumeProcess.Call(uintptr(handle))
	if r != 0 {
		return fmt.Errorf("NtResumeProcess status: %x", r)
	}
	return nil
}
