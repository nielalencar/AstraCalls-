package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestChatwootSourceIDStrategies(t *testing.T) {
	phone := normalizeChatwootPhone(" +55 (51) 99999-9999 ")
	if phone != "5551999999999" {
		t.Fatalf("unexpected normalized phone: %q", phone)
	}
	if got := buildChatwootSourceID("comercial-01", phone, "astra_session_phone"); got != "astra:comercial-01:5551999999999" {
		t.Fatalf("unexpected astra source id: %q", got)
	}
	if got := buildChatwootSourceID("ignored", phone, "phone_only"); got != "whatsapp:5551999999999" {
		t.Fatalf("unexpected phone source id: %q", got)
	}
}

func TestChatwootOutboxDedupRoundtrip(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "dedup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := newSessionStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	item := chatwootOutboxItem{
		SessionID:         "s1",
		SourceMessageID:   "wamid-1",
		Phone:             "5551999999999",
		SourceID:          "astra:s1:5551999999999",
		ChatwootAccountID: 1,
		ChatwootInboxID:   2,
		ChatID:            "5551999999999@s.whatsapp.net",
		Text:              "hello",
	}
	if err := store.enqueueChatwootOutbox(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := store.enqueueChatwootOutbox(ctx, item); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.claimNextChatwootOutbox(ctx, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.SourceMessageID != "wamid-1" {
		t.Fatalf("unexpected claimed item: %+v", claimed)
	}
	if err := store.markChatwootOutboxSent(ctx, *claimed, 456, 789); err != nil {
		t.Fatal(err)
	}
	ok, err := store.chatwootDedupExists(ctx, "s1", "wamid-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("dedup row was not written")
	}
}
