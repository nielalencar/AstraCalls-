package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestSessionStoreCreatesChatwootTables(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "chatwoot_tables.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := newSessionStore(ctx, db); err != nil {
		t.Fatal(err)
	}

	for _, table := range []string{"chatwoot_contact_links", "chatwoot_message_dedup", "chatwoot_outbox", "call_recordings"} {
		var name string
		if err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = $1`, table).Scan(&name); err != nil {
			t.Fatalf("table %s was not created: %v", table, err)
		}
	}
}
