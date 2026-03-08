package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// EddydManager starts and manages eddyd processes for each node.
type EddydManager struct {
	procs    []*exec.Cmd
	monitor  *Monitor
	dataDirs []string
	cfgFiles []string
}

func NewEddydManager(monitor *Monitor) *EddydManager {
	return &EddydManager{monitor: monitor}
}

var rebalanceRe = regexp.MustCompile(`amount_sats=(\d+)`)

func (em *EddydManager) Start(configs []NodeConfig, eddydBin string) error {
	for _, cfg := range configs {
		dataDir := fmt.Sprintf("eddyd-data-%s", cfg.Alias)

		cfgPath := fmt.Sprintf("eddyd-%s.yaml", cfg.Alias)
		cfgContent := fmt.Sprintf(`log_level: debug
data_dir: %s
solver:
  max_cycle_length: 5
  stale_threshold: 60
  min_push_amount: 50000
circulation:
  timeout: 10s
  payment_timeout: 30s
`, dataDir)
		if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
			return fmt.Errorf("write config %s: %w", cfg.Alias, err)
		}

		cmd := exec.Command(eddydBin,
			"--lnd-host", cfg.Host,
			"--lnd-cert", cfg.CertPath,
			"--lnd-macaroon", cfg.MacaroonPath,
			"--config", cfgPath,
		)

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start eddyd %s: %w", cfg.Alias, err)
		}

		em.procs = append(em.procs, cmd)
		em.dataDirs = append(em.dataDirs, dataDir)
		em.cfgFiles = append(em.cfgFiles, cfgPath)
		alias := cfg.Alias
		logPath := filepath.Join(dataDir, "eddy.log")
		em.monitor.AddEvent(fmt.Sprintf("eddyd: started %s (pid %d)", alias, cmd.Process.Pid))

		go func() {
			// Wait for the log file to appear (daemon creates it on startup).
			var f *os.File
			for {
				var err error
				f, err = os.Open(logPath)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			defer f.Close()

			reader := bufio.NewReader(f)
			for {
				line, err := reader.ReadString('\n')
				if len(line) > 0 {
					line = strings.TrimRight(line, "\n")

					if strings.Contains(line, "COMPLETE") {
						if m := rebalanceRe.FindStringSubmatch(line); len(m) > 1 {
							if sats, err := strconv.ParseInt(m[1], 10, 64); err == nil {
								em.monitor.IncrRebalance(sats)
								em.monitor.AddEvent(fmt.Sprintf("%s: rebalance COMPLETE (%s sats)", alias, formatSats(sats)))
							}
						} else {
							em.monitor.AddEvent(fmt.Sprintf("%s: rebalance COMPLETE", alias))
							em.monitor.IncrRebalance(0)
						}
					}
					em.monitor.AddEvent(fmt.Sprintf("[%s] %s", alias, truncate(line, 90)))
				}
				if err != nil {
					if err == io.EOF {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					return
				}
			}
		}()
	}
	return nil
}

func (em *EddydManager) Stop() {
	for _, cmd := range em.procs {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}
	for _, cmd := range em.procs {
		cmd.Wait()
	}
}

// Cleanup removes eddyd data directories and config files.
func (em *EddydManager) Cleanup() {
	for _, d := range em.dataDirs {
		os.RemoveAll(d)
	}
	for _, f := range em.cfgFiles {
		os.Remove(f)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
