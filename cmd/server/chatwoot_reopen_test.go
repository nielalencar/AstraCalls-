package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestChatwootOutboxReopensResolvedConversationBeforeMessage(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "reopen.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := newSessionStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var calls []string
	cw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/accounts/1/conversations/456":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": 456, "inbox_id": 2, "contact_id": 10, "source_id": "astra:s1:5551999999999", "status": "resolved",
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/accounts/1/conversations/456/toggle_status":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 456, "status": "open"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/accounts/1/conversations/456/messages":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 789})
		default:
			http.NotFound(w, r)
		}
	}))
	defer cw.Close()

	mgr := newSessionManager(ctx, nil, NewBroker(), store, waLog.Noop, slog.Default(), 0)
	sess := &Session{id: "s1", mgr: mgr, chatwoot: ChatwootConfig{
		URL: cw.URL, AccountID: 1, AccountToken: "token", InboxID: 2,
	}}
	mgr.register(sess)

	if err := store.upsertChatwootContactLink(ctx, chatwootContactLink{
		SessionID: "s1", Phone: "5551999999999", SourceID: "astra:s1:5551999999999",
		ChatwootAccountID: 1, ChatwootInboxID: 2, ChatwootContactID: 10, ChatwootConversationID: 456,
		LastConversationStatus: "resolved",
	}); err != nil {
		t.Fatal(err)
	}

	item := chatwootOutboxItem{
		ID: "outbox-1", SessionID: "s1", SourceMessageID: "wamid-1", Phone: "5551999999999",
		SourceID: "astra:s1:5551999999999", ChatwootAccountID: 1, ChatwootInboxID: 2,
		ChatID: "5551999999999@s.whatsapp.net", Text: "hello",
	}
	mgr.processChatwootOutboxItem(item)

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"GET /api/v1/accounts/1/conversations/456",
		"POST /api/v1/accounts/1/conversations/456/toggle_status",
		"POST /api/v1/accounts/1/conversations/456/messages",
	}
	if len(calls) != len(want) {
		t.Fatalf("unexpected calls: got %v want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call %d = %q, want %q (all calls: %v)", i, calls[i], want[i], calls)
		}
	}
}
