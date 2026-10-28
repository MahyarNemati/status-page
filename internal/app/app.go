// Package app wires configuration, storage, probes, incident handling, SLO
// evaluation, alerting and the HTTP server into one runnable service.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/incident"
	"github.com/MahyarNemati/status-page/internal/metrics"
	"github.com/MahyarNemati/status-page/internal/monitor"
	"github.com/MahyarNemati/status-page/internal/slo"
	"github.com/MahyarNemati/status-page/internal/store"
	"github.com/MahyarNemati/status-page/internal/web"
)

// Options carries runtime dependencies that tests may replace.
type Options struct {
	Clock  clock.Clock
	Logger *slog.Logger
	// AfterObservation runs after each observation has been fully processed.
	AfterObservation func(monitor.Observation)
}

// App is the assembled service.
type App struct {
	cfg        *config.Config
	opts       Options
	log        *slog.Logger
	store      store.Store
	metrics    *metrics.Metrics
	dispatcher *alert.Dispatcher
	incidents  *incident.Manager
	slo        *slo.Evaluator
	scheduler  *monitor.Scheduler
	web        *web.Server
	targets    []monitor.Target
}

// New builds the service and opens storage. Call Close when done.
func New(ctx context.Context, cfg *config.Config, opts Options) (*App, error) {
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	a := &App{cfg: cfg, opts: opts, log: opts.Logger, metrics: metrics.New()}

	for _, svc := range cfg.Services {
		c, err := check.New(svc)
		if err != nil {
			return nil, fmt.Errorf("service %s: %w", svc.ID, err)
		}
		a.targets = append(a.targets, monitor.Target{Service: svc, Checker: c})
	}

	st, err := store.Open(ctx, cfg.Storage.Driver, cfg.Storage.DSN)
	if err != nil {
		return nil, err
	}
	a.store = st

	notifiers, err := alert.FromConfig(cfg.Alerts)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	a.dispatcher = alert.NewDispatcher(notifiers, alert.DispatcherOptions{
		DedupeWindow: cfg.Alerts.DedupeWindow.D(),
		Timeout:      cfg.Alerts.Timeout.D(),
		Clock:        opts.Clock,
		Logger:       a.log.With("component", "alert"),
		Observer:     a.metrics.ObserveAlert,
	})

	a.incidents = incident.NewManager(st, a.dispatcher, incident.Policy{
		FlapWindow:    cfg.Incidents.FlapWindow.D(),
		FlapThreshold: cfg.Incidents.FlapThreshold,
	}, cfg.Server.PublicURL, a.log.With("component", "incident"))
	if err := a.incidents.Restore(ctx); err != nil {
		_ = st.Close()
		return nil, err
	}

	a.slo = slo.New(slo.Options{
		Store: st, Clock: opts.Clock, Services: cfg.Services, Policy: cfg.SLO,
		Publisher: a.dispatcher, PublicURL: cfg.Server.PublicURL,
		Observer: a.metrics.ObserveSLO, Logger: a.log.With("component", "slo"),
	})

	a.scheduler = monitor.New(monitor.Options{
		Workers: cfg.Scheduler.Workers,
		Jitter:  *cfg.Scheduler.Jitter,
		Clock:   opts.Clock,
		Handler: a.handle,
		Logger:  a.log.With("component", "scheduler"),
	})

	a.web, err = web.New(web.Deps{
		Config: cfg, Store: st, Incidents: a.incidents, SLO: a.slo, Metrics: a.metrics,
		Clock: opts.Clock, Logger: a.log.With("component", "http"),
	})
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	return a, nil
}

// Handler exposes the HTTP handler, mainly for tests.
func (a *App) Handler() http.Handler { return a.web.Handler() }

// Close releases storage.
func (a *App) Close() error { return a.store.Close() }

func (a *App) handle(ctx context.Context, o monitor.Observation) {
	a.metrics.ObserveCheck(o.Service.ID, o.Result)
	err := a.store.SaveResult(ctx, store.Result{
		ServiceID: o.Service.ID, At: o.At, Status: o.Result.Status, Latency: o.Result.Latency, Message: o.Result.Message,
	})
	if err != nil {
		a.log.Error("save result", "service", o.Service.ID, "err", err)
	}
	if err := a.incidents.Observe(ctx, o.Service, o.Result, o.At); err != nil {
		a.log.Error("incident evaluation", "service", o.Service.ID, "err", err)
	}
	a.log.Debug("check complete", "service", o.Service.ID, "status", o.Result.Status,
		"latency", o.Result.Latency, "attempts", o.Attempts, "message", o.Result.Message)
	if a.opts.AfterObservation != nil {
		a.opts.AfterObservation(o)
	}
}

// Run serves HTTP and runs all background loops until ctx is cancelled,
// then shuts down gracefully.
func (a *App) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", a.cfg.Server.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{
		Handler:           a.web.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(a.log.Handler(), slog.LevelWarn),
	}
	a.log.Info("status page listening", "addr", ln.Addr().String(), "services", len(a.targets),
		"storage", a.cfg.Storage.Driver, "admin_api", a.cfg.Server.AdminToken != "")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	var wg sync.WaitGroup
	alertsCtx, stopAlerts := context.WithCancel(context.WithoutCancel(ctx))
	alertsDone := make(chan struct{})
	go func() { a.dispatcher.Run(alertsCtx); close(alertsDone) }()
	for _, loop := range []func(context.Context){
		func(ctx context.Context) { a.scheduler.Run(ctx, a.targets) },
		func(ctx context.Context) { a.slo.Run(ctx, a.cfg.SLO.EvaluateEvery.D()) },
		a.pruneLoop,
		a.gaugeLoop,
	} {
		wg.Add(1)
		go func() { defer wg.Done(); loop(ctx) }()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		runErr = err
		cancel()
	}

	a.log.Info("shutting down")
	shutdownCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer done()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.log.Warn("http shutdown", "err", err)
	}
	wg.Wait()
	// Stop alerts last so notifications produced by the final checks drain.
	stopAlerts()
	<-alertsDone
	return runErr
}

func (a *App) pruneLoop(ctx context.Context) {
	r := a.cfg.Storage.Retention
	for {
		stats, err := a.store.Prune(ctx, a.opts.Clock.Now(), store.Retention{Raw: r.Raw.D(), Hourly: r.Hourly.D(), Daily: r.Daily.D()})
		switch {
		case err != nil && ctx.Err() == nil:
			a.log.Error("prune", "err", err)
		case err == nil && stats.Raw+stats.Hourly+stats.Daily > 0:
			a.log.Info("pruned old data", "raw", stats.Raw, "hourly", stats.Hourly, "daily", stats.Daily)
		}
		if !clock.Sleep(a.opts.Clock, r.Every.D(), ctx.Done()) {
			return
		}
	}
}

// gaugeLoop refreshes the open incident gauge, which includes incidents
// declared or resolved through the admin API.
func (a *App) gaugeLoop(ctx context.Context) {
	for {
		open, err := a.store.ListIncidents(ctx, store.IncidentFilter{State: store.IncidentOpen})
		if err == nil {
			counts := map[string]int{}
			for _, in := range open {
				counts[in.ServiceID]++
			}
			for _, svc := range a.cfg.Services {
				a.metrics.SetOpenIncidents(svc.ID, counts[svc.ID])
			}
		} else if ctx.Err() == nil {
			a.log.Error("refresh incident gauge", "err", err)
		}
		if !clock.Sleep(a.opts.Clock, 30*time.Second, ctx.Done()) {
			return
		}
	}
}
