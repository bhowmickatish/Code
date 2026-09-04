package zk

import (
	"context"
	"fmt"
	"log"
	"os"

	gzk "github.com/go-zookeeper/zk"

	"github.com/atish/go-zookeeper/internal/model"
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

func (l *Loader) LoadOnStartup(ctx context.Context, bootstrap bool) (model.RulesDocument, error) {
	if bootstrap {
		if err := l.bootstrapIfMissing(); err != nil {
			return model.RulesDocument{}, err
		}
	}

	doc, err := l.load()
	if err != nil {
		return model.RulesDocument{}, err
	}

	log.Printf("loaded %d rate limit rules from %s (version=%d)", len(doc.Rules), l.rulesPath, doc.Version)
	for _, rule := range doc.Rules {
		log.Printf("  rule %q: prefix=%s limit=%d window=%s key=%s",
			rule.Name, rule.PathPrefix, rule.Limit, rule.Window, rule.Key)
	}
	return doc, nil
}

func (l *Loader) Watch(ctx context.Context, onReload func(model.RulesDocument)) error {
	for {
		ch, err := l.client.Watch(l.rulesPath)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-ch:
			if event.Type != gzk.EventNodeDataChanged && event.Type != gzk.EventNodeCreated {
				continue
			}

			doc, err := l.load()
			if err != nil {
				log.Printf("reload rules failed: %v", err)
				continue
			}

			log.Printf("reloaded %d rate limit rules from zookeeper (version=%d)", len(doc.Rules), doc.Version)
			onReload(doc)
		}
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

	log.Printf("bootstrapped rate limit rules at %s from %s", l.rulesPath, l.seedFile)
	return nil
}

func (l *Loader) load() (model.RulesDocument, error) {
	data, err := l.client.Get(l.rulesPath)
	if err != nil {
		return model.RulesDocument{}, err
	}
	return model.ParseRulesDocument(data)
}
