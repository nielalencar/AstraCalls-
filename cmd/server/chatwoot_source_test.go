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

func TestChatwootPhoneForPeerDoesNotTreatLIDAsPhone(t *testing.T) {
	sess := &Session{}
	if got := sess.chatwootPhoneForPeer("273422517063840@lid"); got != "" {
		t.Fatalf("lid must not be treated as phone: %q", got)
	}
	if got := sess.chatwootPhoneForPeer("555189632063@s.whatsapp.net"); got != "555189632063" {
		t.Fatalf("unexpected phone jid: %q", got)
	}
}

func TestChatwootSourceIDMatchesPhone(t *testing.T) {
	if !chatwootSourceIDMatchesPhone("astra:s1:555189632063", "555189632063") {
		t.Fatal("expected astra source id to match phone")
	}
	if !chatwootSourceIDMatchesPhone("whatsapp:555189632063", "555189632063") {
		t.Fatal("expected phone_only source id to match phone")
	}
	if chatwootSourceIDMatchesPhone("astra:s1:273422517063840", "555189632063") {
		t.Fatal("must not reuse source id built from a lid")
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
