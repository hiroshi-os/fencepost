package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type ChaosResult struct {
	Seed                int64         `json:"Seed"`
	Jobs                int           `json:"Jobs"`
	Workers             int           `json:"Workers"`
	Fencing             bool          `json:"Fencing"`
	Duration            time.Duration `json:"Duration"`
	Kills               int64         `json:"Kills"`
	Pauses              int64         `json:"Pauses"`
	ServerRestarts      int64         `json:"ServerRestarts"`
	PGRestarts          int64         `json:"PGRestarts"`
	TotalExecutions     int64         `json:"TotalExecutions"`
	DuplicateExecutions int64         `json:"DuplicateExecutions"`
	DuplicateEffects    int64         `json:"DuplicateEffects"`
	LostJobs            int64         `json:"LostJobs"`
	SucceededJobs       int64         `json:"SucceededJobs"`
	DeadJobs            int64         `json:"DeadJobs"`
	StaleTokenServer    int64         `json:"StaleTokenServer"`
	StaleTokenSink      int64         `json:"StaleTokenSink"`
}

func main() {
	jobs := flag.Int("jobs", 2000, "jobs per seed")
	workers := flag.Int("workers", 8, "workers")
	dbURL := flag.String("db", "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable", "database url")
	binDir := flag.String("bin-dir", "bin", "bin dir")
	flag.Parse()

	absBinDir, err := filepath.Abs(*binDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to resolve bin-dir: %v\n", err)
		os.Exit(1)
	}

	chaosBin := filepath.Join(absBinDir, "chaos.exe")
	if _, err := os.Stat(chaosBin); err != nil {
		chaosBin = filepath.Join(absBinDir, "chaos")
	}

	seeds := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	fmt.Println("=== RUNNING CHAOS MATRIX: FENCING ON ===")
	var onResults []*ChaosResult
	for _, s := range seeds {
		fmt.Printf("--> Running seed %d (fencing ON)...\n", s)
		cmd := exec.Command(chaosBin,
			"-bin-dir", absBinDir,
			"-seed", fmt.Sprintf("%d", s),
			"-jobs", fmt.Sprintf("%d", *jobs),
			"-workers", fmt.Sprintf("%d", *workers),
			"-db", *dbURL,
			"-json",
		)
		out, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Seed %d failed: %v, out: %s\n", s, err, string(out))
			os.Exit(1)
		}
		var r ChaosResult
		if err := json.Unmarshal(out, &r); err != nil {
			fmt.Fprintf(os.Stderr, "Parse seed %d failed: %v, out: %s\n", s, err, string(out))
			os.Exit(1)
		}
		onResults = append(onResults, &r)
		fmt.Printf("    Seed %d: drain=%.2fs, pauses=%d, kills=%d, dup_exec=%d, dup_effects=%d, lost=%d, stale_rejections=%d\n",
			r.Seed, r.Duration.Seconds(), r.Pauses, r.Kills, r.DuplicateExecutions, r.DuplicateEffects, r.LostJobs, r.StaleTokenSink)
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\n=== RUNNING CHAOS MATRIX: FENCING OFF (NEGATIVE CONTROL) ===")
	var offResults []*ChaosResult
	for _, s := range seeds {
		fmt.Printf("--> Running seed %d (fencing OFF)...\n", s)
		cmd := exec.Command(chaosBin,
			"-bin-dir", absBinDir,
			"-seed", fmt.Sprintf("%d", s),
			"-jobs", fmt.Sprintf("%d", *jobs),
			"-workers", fmt.Sprintf("%d", *workers),
			"-db", *dbURL,
			"-no-fencing",
			"-json",
		)
		out, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Seed %d (off) failed: %v, out: %s\n", s, err, string(out))
			os.Exit(1)
		}
		var r ChaosResult
		if err := json.Unmarshal(out, &r); err != nil {
			fmt.Fprintf(os.Stderr, "Parse seed %d (off) failed: %v, out: %s\n", s, err, string(out))
			os.Exit(1)
		}
		offResults = append(offResults, &r)
		fmt.Printf("    Seed %d (off): drain=%.2fs, pauses=%d, kills=%d, dup_exec=%d, dup_effects=%d (NEGATIVE CONTROL HIT!), lost=%d\n",
			r.Seed, r.Duration.Seconds(), r.Pauses, r.Kills, r.DuplicateExecutions, r.DuplicateEffects, r.LostJobs)
		time.Sleep(1 * time.Second)
	}

	// Print Markdown tables
	printMarkdown(onResults, offResults)
}

func printMarkdown(on, off []*ChaosResult) {
	fmt.Println("\n### Chaos Matrix Results (Fencing ON)")
	fmt.Println("| Seed | Jobs | Duration | Kills | Pauses | Replica Restarts | PG Hiccups | Total Executions | Dupl Executions | Duplicate Effects | Lost Jobs | Stale Rejections |")
	fmt.Println("|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|")

	var totJobs, totKills, totPauses, totRestarts, totPG, totExecs, totDupExecs, totDupEffects, totLost, totRejections int64
	var totDuration time.Duration

	for _, r := range on {
		totJobs += int64(r.Jobs)
		totDuration += r.Duration
		totKills += r.Kills
		totPauses += r.Pauses
		totRestarts += r.ServerRestarts
		totPG += r.PGRestarts
		totExecs += r.TotalExecutions
		totDupExecs += r.DuplicateExecutions
		totDupEffects += r.DuplicateEffects
		totLost += r.LostJobs
		totRejections += r.StaleTokenSink

		fmt.Printf("| %d | %d | %.2fs | %d | %d | %d | %d | %d | %d | **%d** | **%d** | %d |\n",
			r.Seed, r.Jobs, r.Duration.Seconds(), r.Kills, r.Pauses, r.ServerRestarts, r.PGRestarts,
			r.TotalExecutions, r.DuplicateExecutions, r.DuplicateEffects, r.LostJobs, r.StaleTokenSink)
	}
	fmt.Printf("| **TOTAL** | **%d** | **%.2fs** | **%d** | **%d** | **%d** | **%d** | **%d** | **%d** | **%d** | **%d** | **%d** |\n\n",
		totJobs, totDuration.Seconds(), totKills, totPauses, totRestarts, totPG, totExecs, totDupExecs, totDupEffects, totLost, totRejections)

	fmt.Println("### Negative Control Results (Fencing OFF, Same Seeds)")
	fmt.Println("| Seed | Jobs | Duration | Kills | Pauses | Replica Restarts | Total Executions | Dupl Executions | Duplicate Effects (Unfenced) | Lost Jobs |")
	fmt.Println("|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|")

	var offJobs, offKills, offPauses, offRestarts, offExecs, offDupExecs, offDupEffects, offLost int64
	var offDuration time.Duration

	for _, r := range off {
		offJobs += int64(r.Jobs)
		offDuration += r.Duration
		offKills += r.Kills
		offPauses += r.Pauses
		offRestarts += r.ServerRestarts
		offExecs += r.TotalExecutions
		offDupExecs += r.DuplicateExecutions
		offDupEffects += r.DuplicateEffects
		offLost += r.LostJobs

		fmt.Printf("| %d | %d | %.2fs | %d | %d | %d | %d | %d | **%d** | **%d** |\n",
			r.Seed, r.Jobs, r.Duration.Seconds(), r.Kills, r.Pauses, r.ServerRestarts,
			r.TotalExecutions, r.DuplicateExecutions, r.DuplicateEffects, r.LostJobs)
	}
	fmt.Printf("| **TOTAL** | **%d** | **%.2fs** | **%d** | **%d** | **%d** | **%d** | **%d** | **%d** | **%d** |\n\n",
		offJobs, offDuration.Seconds(), offKills, offPauses, offRestarts, offExecs, offDupExecs, offDupEffects, offLost)
}
