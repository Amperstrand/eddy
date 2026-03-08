package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	cfg := loadConfig("/nonexistent/path/config.yaml", false)
	if cfg.Solver.MaxCycleLength != 5 {
		t.Fatalf("expected max_cycle_length 5, got %d", cfg.Solver.MaxCycleLength)
	}
	if cfg.Solver.StaleThreshold != 60 {
		t.Fatalf("expected stale_threshold 60, got %d", cfg.Solver.StaleThreshold)
	}
	if cfg.Solver.MinPushAmount != 100000 {
		t.Fatalf("expected min_push_amount 100000, got %d", cfg.Solver.MinPushAmount)
	}
	if cfg.Circulation.Timeout != 30*time.Second {
		t.Fatalf("expected 30s timeout, got %v", cfg.Circulation.Timeout)
	}
	if cfg.Circulation.PaymentTimeout != 60*time.Second {
		t.Fatalf("expected 60s payment timeout, got %v", cfg.Circulation.PaymentTimeout)
	}
	if cfg.Gossip.Interval != 30*time.Second {
		t.Fatalf("expected 30s gossip interval, got %v", cfg.Gossip.Interval)
	}
}

func TestLoadConfig_FromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
solver:
  max_cycle_length: 3
  stale_threshold: 120
  min_push_amount: 50000
circulation:
  timeout: 60s
  payment_timeout: 90s
gossip:
  interval: 45s
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := loadConfig(path, true)
	if cfg.Solver.MaxCycleLength != 3 {
		t.Fatalf("expected 3, got %d", cfg.Solver.MaxCycleLength)
	}
	if cfg.Solver.StaleThreshold != 120 {
		t.Fatalf("expected 120, got %d", cfg.Solver.StaleThreshold)
	}
	if cfg.Solver.MinPushAmount != 50000 {
		t.Fatalf("expected 50000, got %d", cfg.Solver.MinPushAmount)
	}
	if cfg.Circulation.Timeout != 60*time.Second {
		t.Fatalf("expected 60s, got %v", cfg.Circulation.Timeout)
	}
	if cfg.Circulation.PaymentTimeout != 90*time.Second {
		t.Fatalf("expected 90s payment timeout, got %v", cfg.Circulation.PaymentTimeout)
	}
	if cfg.Gossip.Interval != 45*time.Second {
		t.Fatalf("expected 45s gossip interval, got %v", cfg.Gossip.Interval)
	}
}

func TestConfigDefaults_LogLevel(t *testing.T) {
	cfg := loadConfig("/nonexistent/path/config.yaml", false)
	if cfg.LogLevel != "info" {
		t.Fatalf("expected default log_level info, got %s", cfg.LogLevel)
	}
}

func TestExpandHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home dir")
	}
	got := expandHome("~/test")
	want := filepath.Join(home, "test")
	if got != want {
		t.Fatalf("expandHome(~/test) = %s, want %s", got, want)
	}

	if expandHome("/absolute/path") != "/absolute/path" {
		t.Fatal("absolute path should be unchanged")
	}
}
