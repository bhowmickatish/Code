package zk

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	gzk "github.com/go-zookeeper/zk"

	"github.com/atish/go-zookeeper/internal/model"
	"github.com/atish/go-zookeeper/internal/reloadstatus"
)

type Loader struct {
	client    *Client
	rulesPath string
	seedFile  string
}

func NewLoader(client *Client, rulesPath, seedFile string) *Loader {
	return &Loader{
		client:    client,
		rulesPath: rulesPath,
		seedFile:  seedFile,
	}
}

// LoadOnStartup loads rules from ZooKeeper, falling back to seedFile when ZK is unavailable.
func (l *Loader) LoadOnStartup(ctx context.Context, bootstrap bool) (model.RulesDocument, error) {
	if err := ctx.Err(); err != nil {
		return model.RulesDocument{}, err
	}

	if bootstrap {
		if err := l.bootstrapIfMissing(); err != nil {
			slog.Warn("zookeeper bootstrap skipped", "err", err)
		}
	}

	if err := ctx.Err(); err != nil {
		return model.RulesDocument{}, err
	}

	doc, err := l.load()
	if err != nil {
		reloadstatus.RecordZK("zookeeper", err)
		slog.Warn("zookeeper load failed, using fallback file", "file", l.seedFile, "err", err)
		return LoadRulesFromFile(l.seedFile)
	}

	reloadstatus.RecordZK("zookeeper", nil)
	logRules(doc, l.rulesPath)
	return doc, nil
}

type ReloadFunc func(model.RulesDocument) error

func applyReload(onReload ReloadFunc, doc model.RulesDocument, source string) {
	err := onReload(doc)
	reloadstatus.RecordRulesApply(source, err)
	if err != nil {
		slog.Error("apply rate limit rules failed", "source", source, "err", err)
	}
}

func (l *Loader) Watch(ctx context.Context, onReload ReloadFunc) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		ch, err := l.client.Watch(l.rulesPath)
		if err != nil {
			reloadErr := fmt.Errorf("register watch on %q: %w", l.rulesPath, err)
			reloadstatus.RecordZK("zookeeper-watch", reloadErr)
			slog.Warn("zookeeper watch registration failed, applying fallback rules", "err", err)
			l.applyFallback(onReload)
			if !sleepOrDone(ctx, 2*time.Second) {
				return ctx.Err()
			}
			continue
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case sessEvent := <-l.client.Events():
				if l.handleSessionEvent(sessEvent, onReload) {
					goto rewatch
				}

			case event := <-ch:
				if l.handleNodeEvent(event, onReload) {
					goto rewatch
				}
			}
		}
	rewatch:
	}
}

// handleSessionEvent handles session lifecycle events. Returns true when the node
// watch should be re-registered (e.g. after session expiry).
func (l *Loader) handleSessionEvent(event gzk.Event, onReload ReloadFunc) bool {
	if event.Type != gzk.EventSession {
		return false
	}

	switch event.State {
	case gzk.StateExpired:
		reloadstatus.RecordZK("zookeeper-session", fmt.Errorf("session expired"))
		slog.Warn("zookeeper session expired, applying fallback rules", "file", l.seedFile)
		l.applyFallback(onReload)
		return true
	case gzk.StateDisconnected:
		slog.Warn("zookeeper disconnected, waiting for reconnect")
	case gzk.StateConnected:
		slog.Info("zookeeper session connected, reloading rules from zookeeper")
		l.reloadFromZK(onReload)
	}
	return false
}

func (l *Loader) handleNodeEvent(event gzk.Event, onReload ReloadFunc) bool {
	switch event.Type {
	case gzk.EventNodeDataChanged, gzk.EventNodeCreated:
		l.reloadFromZK(onReload)
	case gzk.EventNodeDeleted:
		slog.Warn("zookeeper rules node deleted, applying fallback rules", "path", l.rulesPath, "file", l.seedFile)
		l.applyFallback(onReload)
		return true
	case gzk.EventNotWatching:
		slog.Warn("zookeeper watch lost, re-registering", "path", l.rulesPath)
		return true
	}
	return false
}

func (l *Loader) reloadFromZK(onReload ReloadFunc) {
	doc, err := l.load()
	if err != nil {
		reloadstatus.RecordZK("zookeeper", err)
		slog.Warn("reload rules from zookeeper failed, keeping current rules", "err", err)
		return
	}
	reloadstatus.RecordZK("zookeeper", nil)
	slog.Info("reloaded rate limit rules from zookeeper", "count", len(doc.Rules), "version", doc.Version)
	applyReload(onReload, doc, "zookeeper")
}

func (l *Loader) applyFallback(onReload ReloadFunc) {
	doc, err := LoadRulesFromFile(l.seedFile)
	if err != nil {
		reloadstatus.RecordRulesApply("fallback", err)
		slog.Error("fallback rules file unavailable", "file", l.seedFile, "err", err)
		return
	}
	applyReload(onReload, doc, "fallback")
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (l *Loader) bootstrapIfMissing() error {
	exists, err := l.client.Exists(l.rulesPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	data, err := os.ReadFile(l.seedFile)
	if err != nil {
		return fmt.Errorf("read seed rules file %q: %w", l.seedFile, err)
	}

	if _, err := model.ParseRulesDocument(data); err != nil {
		return fmt.Errorf("validate seed rules: %w", err)
	}

	if err := l.client.CreatePath(l.rulesPath, data); err != nil {
		return fmt.Errorf("bootstrap rules at %q: %w", l.rulesPath, err)
	}

	slog.Info("bootstrapped rate limit rules in zookeeper", "path", l.rulesPath, "file", l.seedFile)
	return nil
}

func (l *Loader) load() (model.RulesDocument, error) {
	if testLoadHook != nil {
		return testLoadHook(l)
	}

	data, err := l.client.Get(l.rulesPath)
	if err != nil {
		return model.RulesDocument{}, err
	}
	return model.ParseRulesDocument(data)
}

// LoadRulesFromFile loads and validates rules from a local JSON file.
func LoadRulesFromFile(path string) (model.RulesDocument, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.RulesDocument{}, fmt.Errorf("read rules file %q: %w", path, err)
	}

	doc, err := model.ParseRulesDocument(data)
	if err != nil {
		return model.RulesDocument{}, fmt.Errorf("parse rules file %q: %w", path, err)
	}

	logRules(doc, path)
	return doc, nil
}

func logRules(doc model.RulesDocument, source string) {
	slog.Info("loaded rate limit rules",
		"source", source,
		"count", len(doc.Rules),
		"version", doc.Version,
	)
	for _, rule := range doc.Rules {
		slog.Info("rate limit rule",
			"name", rule.Name,
			"path_prefix", rule.PathPrefix,
			"limit", rule.Limit,
			"window", rule.Window,
			"key", rule.Key,
		)
	}
}
