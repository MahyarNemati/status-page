// Package slo turns probe history into availability objectives and
// burn-rate alerts.
//
// The model is the standard error-budget one. An objective of 99.9% over 30
// days permits 0.1% of probes to fail; that 0.1% is the error budget. Burn
// rate is how fast the budget is being spent relative to the rate that would
// exhaust it exactly at the end of the window: a burn rate of 1 uses the
// whole budget over the whole window, a burn rate of 14.4 uses it in a day.
//
// Alerts use the multi-window technique from the Google SRE workbook: a rule
// fires only when both a long and a short window exceed the threshold. The
// long window supplies significance and the short one supplies freshness, so
// a brief spike does not page and a resolved outage stops paging promptly.
package slo

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/store"
)

// Status is an objective evaluated over its window.
type Status struct {
	ServiceID string `json:"service_id"`
	// Target is the objective in percent, for example 99.9.
	Target float64       `json:"target"`
	Window time.Duration `json:"window_ns"`
	// Availability is the measured percentage, or -1 when there is no data.
	Availability float64 `json:"availability"`
	// BudgetConsumed is the fraction of the error budget spent, where 1 means
	// exhausted. It can exceed 1 after a large outage.
	BudgetConsumed float64 `json:"budget_consumed"`
	// BudgetRemaining is 1-BudgetConsumed, floored at 0.
	BudgetRemaining float64 `json:"budget_remaining"`
	// Breaching reports whether measured availability is below Target.
	Breaching bool         `json:"breaching"`
	Counts    store.Counts `json:"counts"`
}

// HasData reports whether any probes landed in the window.
func (s Status) HasData() bool { return s.Counts.Total > 0 }

// ErrorBudget returns the permitted failure ratio for a target in percent.
// A target of 99.9 permits 0.001.
func ErrorBudget(target float64) float64 {
	return math.Max(0, (100-target)/100)
}

// Evaluate computes the objective for one service over [now-window, now].
func Evaluate(ctx context.Context, st store.Store, serviceID string, target float64, window time.Duration, now time.Time) (Status, error) {
	if window <= 0 {
		return Status{}, fmt.Errorf("slo: window must be positive, got %s", window)
	}
	if target <= 0 || target > 100 {
		return Status{}, fmt.Errorf("slo: target must be in (0,100], got %v", target)
	}

	counts, err := st.RawCounts(ctx, serviceID, now.Add(-window), now)
	if err != nil {
		return Status{}, fmt.Errorf("slo: counts for %s: %w", serviceID, err)
	}

	s := Status{
		ServiceID: serviceID,
		Target:    target,
		Window:    window,
		Counts:    counts,
	}
	ratio := counts.Ratio()
	if ratio < 0 {
		// No data is not 0% availability. Reporting a fresh service as
		// breaching would page on startup.
		s.Availability = -1
		s.BudgetRemaining = 1
		return s, nil
	}

	s.Availability = ratio * 100
	budget := ErrorBudget(target)
	errorRatio := 1 - ratio
	if budget == 0 {
		// A 100% objective has no budget: any failure exhausts it.
		if errorRatio > 0 {
			s.BudgetConsumed = math.Inf(1)
		}
	} else {
		s.BudgetConsumed = errorRatio / budget
	}
	s.BudgetRemaining = math.Max(0, 1-s.BudgetConsumed)
	// Compare in percent with a small tolerance so float noise at exactly the
	// target does not flip the verdict.
	s.Breaching = s.Availability < target-1e-9
	return s, nil
}

// BurnRate is the burn rate measured over one window.
type BurnRate struct {
	Window time.Duration `json:"window_ns"`
	// Rate is error ratio divided by error budget. Zero when no probes
	// landed in the window.
	Rate   float64      `json:"rate"`
	Counts store.Counts `json:"counts"`
}

// Alert is a fired burn-rate rule.
type Alert struct {
	ServiceID string   `json:"service_id"`
	Rule      string   `json:"rule"`
	Severity  string   `json:"severity"`
	Factor    float64  `json:"factor"`
	Long      BurnRate `json:"long"`
	Short     BurnRate `json:"short"`
}

// Describe renders the alert as a human sentence for a notification body.
func (a Alert) Describe() string {
	return fmt.Sprintf(
		"Error budget burning at %.1fx over %s and %.1fx over %s (threshold %.1fx). At this rate the budget is exhausted in %s.",
		a.Long.Rate, humanWindow(a.Long.Window),
		a.Short.Rate, humanWindow(a.Short.Window),
		a.Factor, a.exhaustion(),
	)
}

func (a Alert) exhaustion() string {
	if a.Long.Rate <= 0 {
		return "no time at all"
	}
	return humanWindow(time.Duration(float64(a.Long.Window) / a.Long.Rate))
}

// burnRate measures the burn rate over [now-window, now].
func burnRate(ctx context.Context, st store.Store, serviceID string, target float64, window time.Duration, now time.Time) (BurnRate, error) {
	counts, err := st.RawCounts(ctx, serviceID, now.Add(-window), now)
	if err != nil {
		return BurnRate{}, fmt.Errorf("slo: burn counts for %s: %w", serviceID, err)
	}
	br := BurnRate{Window: window, Counts: counts}
	ratio := counts.Ratio()
	if ratio < 0 {
		return br, nil // no data burns nothing
	}
	budget := ErrorBudget(target)
	errorRatio := 1 - ratio
	switch {
	case budget > 0:
		br.Rate = errorRatio / budget
	case errorRatio > 0:
		br.Rate = math.Inf(1)
	}
	return br, nil
}

// EvaluateBurn checks every rule and returns those that fire. A rule fires
// only when the long and short windows both exceed Factor.
//
// Rules are evaluated in order and only the first (most severe) match is
// returned, so one outage produces one page rather than one per rule. Order
// your rules fastest-burn first.
func EvaluateBurn(ctx context.Context, st store.Store, serviceID string, target float64, rules []config.BurnRule, now time.Time) ([]Alert, error) {
	var fired []Alert
	for _, r := range rules {
		long, short := r.Long.D(), r.Short.D()
		if long <= 0 || short <= 0 || r.Factor <= 0 {
			return nil, fmt.Errorf("slo: burn rule %q needs positive long, short and factor", r.Name)
		}
		if short > long {
			return nil, fmt.Errorf("slo: burn rule %q has short window (%s) longer than long window (%s)", r.Name, short, long)
		}

		lb, err := burnRate(ctx, st, serviceID, target, long, now)
		if err != nil {
			return nil, err
		}
		// Require data in the long window: an empty window must not fire.
		if lb.Counts.Total == 0 || lb.Rate < r.Factor {
			continue
		}
		sb, err := burnRate(ctx, st, serviceID, target, short, now)
		if err != nil {
			return nil, err
		}
		if sb.Counts.Total == 0 || sb.Rate < r.Factor {
			// Long window is hot but the short one has cooled: the outage is
			// over. Not firing here is what stops a resolved incident paging
			// for the rest of the long window.
			continue
		}

		sev := r.Severity
		if sev == "" {
			sev = "warning"
		}
		fired = append(fired, Alert{
			ServiceID: serviceID,
			Rule:      r.Name,
			Severity:  sev,
			Factor:    r.Factor,
			Long:      lb,
			Short:     sb,
		})
		break
	}
	return fired, nil
}

func humanWindow(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d >= 24*time.Hour:
		days := d.Hours() / 24
		if days >= 2 {
			return fmt.Sprintf("%.0fd", days)
		}
		return fmt.Sprintf("%.1fd", days)
	case d >= time.Hour:
		return fmt.Sprintf("%.0fh", d.Hours())
	case d >= time.Minute:
		return fmt.Sprintf("%.0fm", d.Minutes())
	default:
		return d.Round(time.Second).String()
	}
}
