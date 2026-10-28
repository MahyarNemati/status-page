// Command statuspage runs the uptime monitor and public status page.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/MahyarNemati/status-page/internal/app"
	"github.com/MahyarNemati/status-page/internal/config"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "statuspage:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  = flag.String("config", envOr("STATUSPAGE_CONFIG", "config.yaml"), "path to the YAML config file")
		checkOnly   = flag.Bool("check-config", false, "validate the config and exit")
		healthcheck = flag.Bool("healthcheck", false, "probe the local /healthz endpoint and exit; for container health checks")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}
	if *healthcheck {
		return probe(envOr("STATUSPAGE_HEALTHCHECK_URL", "http://127.0.0.1:8080/healthz"))
	}

	logger, err := newLogger(os.Getenv("STATUSPAGE_LOG_LEVEL"), os.Getenv("STATUSPAGE_LOG_FORMAT"))
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *checkOnly {
		fmt.Printf("config OK: %d services, %d alert channels\n", len(cfg.Services), len(cfg.Alerts.Channels))
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, cfg, app.Options{Logger: logger})
	if err != nil {
		return err
	}
	defer a.Close()
	logger.Info("starting", "version", version, "config", *configPath)
	return a.Run(ctx)
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if level != "" {
		if err := lvl.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("STATUSPAGE_LOG_LEVEL: %w", err)
		}
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch strings.ToLower(format) {
	case "", "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, opts)), nil
	default:
		return nil, errors.New("STATUSPAGE_LOG_FORMAT must be json or text")
	}
}

func probe(url string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %d", resp.StatusCode)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
