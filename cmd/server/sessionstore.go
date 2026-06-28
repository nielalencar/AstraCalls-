package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

type sessionRow struct {
	ID       string
	Name     string
	JID      string
	Webhook  string
	Chatwoot string
}

type sessionStore struct{ db *sql.DB }

type chatwootContactLink struct {
	SessionID              string
	Phone                  string
	SourceID               string
	ChatwootAccountID      int
	ChatwootInboxID        int
	ChatwootContactID      int
	ChatwootConversationID int
	LastConversationStatus string
	LastMessageAt          int64
}

type chatwootOutboxItem struct {
	ID                     string
	SessionID              string
	SourceMessageID        string
	Phone                  string
	SourceID               string
	ChatwootAccountID      int
	ChatwootInboxID        int
	ChatID                 string
	Name                   string
	Text                   string
	Filename               string
	Mimetype               string
	MediaBytes             []byte
	Status                 string
	Attempts               int
	NextRetryAt            int64
	LastError              string
	ChatwootConversationID int
	ChatwootMessageID      int
	CreatedAt              int64
	UpdatedAt              int64
}

type callRecordingRow struct {
	ID                     string `json:"id"`
	SessionID              string `json:"session_id"`
	CallID                 string `json:"call_id"`
	Direction              string `json:"direction"`
	Peer                   string `json:"peer"`
	Phone                  string `json:"phone"`
	SourceID               string `json:"source_id"`
	Owner                  string `json:"owner"`
	ChatwootAccountID      int    `json:"chatwoot_account_id"`
	ChatwootInboxID        int    `json:"chatwoot_inbox_id"`
	ChatwootContactID      int    `json:"chatwoot_contact_id"`
	ChatwootConversationID int    `json:"chatwoot_conversation_id"`
	ChatwootMessageID      int    `json:"chatwoot_message_id"`
	Status                 string `json:"status"`
	FilePath               string `json:"-"`
	FileName               string `json:"file_name"`
	MimeType               string `json:"mime_type"`
	SizeBytes              int64  `json:"size_bytes"`
	DurationMs             int64  `json:"duration_ms"`
	StartedAt              int64  `json:"started_at"`
	EndedAt                int64  `json:"ended_at"`
	Error                  string `json:"error"`
	UploadAttempts         int    `json:"upload_attempts"`
	LastUploadAttemptAt    int64  `json:"last_upload_attempt_at"`
	CreatedAt              int64  `json:"created_at"`
	UpdatedAt              int64  `json:"updated_at"`
}

// newSessionStore cria a tabela de config das sessões no banco PRINCIPAL.
// (O store do whatsmeow de cada sessão fica em um banco separado — ver db.go.)
func newSessionStore(ctx context.Context, db *sql.DB) (*sessionStore, error) {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS sessions (
		id         TEXT PRIMARY KEY,
		name       TEXT NOT NULL,
		jid        TEXT,
		webhook    TEXT,
		chatwoot   TEXT,
		created_at BIGINT NOT NULL DEFAULT 0
	)`)
	if err != nil {
		return nil, err
	}
	// migração p/ bancos antigos (Postgres aceita IF NOT EXISTS no ADD COLUMN)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS webhook TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS chatwoot TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0`)
	if err := ensureChatwootTables(ctx, db); err != nil {
		return nil, err
	}
	return &sessionStore{db: db}, nil
}

func ensureChatwootTables(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS chatwoot_contact_links (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			phone TEXT NOT NULL,
			source_id TEXT NOT NULL,
			chatwoot_account_id INTEGER NOT NULL,
			chatwoot_inbox_id INTEGER NOT NULL,
			chatwoot_contact_id INTEGER,
			chatwoot_conversation_id INTEGER,
			last_conversation_status TEXT,
			last_message_at BIGINT,
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL,
			UNIQUE(session_id, phone, chatwoot_account_id, chatwoot_inbox_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chatwoot_contact_links_source_id ON chatwoot_contact_links(source_id)`,
		`CREATE INDEX IF NOT EXISTS idx_chatwoot_contact_links_conversation ON chatwoot_contact_links(chatwoot_conversation_id)`,
		`CREATE TABLE IF NOT EXISTS chatwoot_message_dedup (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			source_message_id TEXT NOT NULL,
			phone TEXT NOT NULL,
			chatwoot_conversation_id INTEGER,
			chatwoot_message_id INTEGER,
			created_at BIGINT NOT NULL,
			UNIQUE(session_id, source_message_id)
		)`,
		`CREATE TABLE IF NOT EXISTS chatwoot_outbox (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			source_message_id TEXT NOT NULL,
			phone TEXT NOT NULL,
			source_id TEXT NOT NULL,
			chatwoot_account_id INTEGER NOT NULL,
			chatwoot_inbox_id INTEGER NOT NULL,
			chat_id TEXT NOT NULL,
			name TEXT,
			text TEXT,
			filename TEXT,
			mimetype TEXT,
			media_bytes BYTEA,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_retry_at BIGINT NOT NULL,
			last_error TEXT,
			chatwoot_conversation_id INTEGER,
			chatwoot_message_id INTEGER,
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL,
			UNIQUE(session_id, source_message_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chatwoot_outbox_status_retry ON chatwoot_outbox(status, next_retry_at)`,
		`CREATE TABLE IF NOT EXISTS call_recordings (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			call_id TEXT NOT NULL,
			direction TEXT,
			peer TEXT,
			phone TEXT,
			source_id TEXT,
			owner TEXT,
			chatwoot_account_id INTEGER,
			chatwoot_inbox_id INTEGER,
			chatwoot_contact_id INTEGER,
			chatwoot_conversation_id INTEGER,
			chatwoot_message_id INTEGER,
			status TEXT NOT NULL DEFAULT 'pending',
			file_path TEXT,
			file_name TEXT,
			mime_type TEXT,
			size_bytes BIGINT DEFAULT 0,
			duration_ms BIGINT DEFAULT 0,
			started_at BIGINT,
			ended_at BIGINT,
			error TEXT,
			upload_attempts INTEGER DEFAULT 0,
			last_upload_attempt_at BIGINT,
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_call_recordings_call_id ON call_recordings(call_id)`,
		`CREATE INDEX IF NOT EXISTS idx_call_recordings_status ON call_recordings(status)`,
		`CREATE INDEX IF NOT EXISTS idx_call_recordings_conversation ON call_recordings(chatwoot_conversation_id)`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func newSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newStoreID() string { return newSessionID() }

func (s *sessionStore) list(ctx context.Context) ([]sessionRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, COALESCE(jid, ''), COALESCE(webhook, ''), COALESCE(chatwoot, '') FROM sessions ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sessionRow
	for rows.Next() {
		var r sessionRow
		if err := rows.Scan(&r.ID, &r.Name, &r.JID, &r.Webhook, &r.Chatwoot); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sessionStore) insert(ctx context.Context, id, name string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions (id, name, jid) VALUES ($1, $2, NULL)`, id, name)
	return err
}

func (s *sessionStore) setJID(ctx context.Context, id, jid string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET jid = $1 WHERE id = $2`, jid, id)
	return err
}

func (s *sessionStore) setWebhook(ctx context.Context, id, url string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET webhook = $1 WHERE id = $2`, url, id)
	return err
}

func (s *sessionStore) setChatwoot(ctx context.Context, id, cfgJSON string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET chatwoot = $1 WHERE id = $2`, cfgJSON, id)
	return err
}

func (s *sessionStore) delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE id = $1`, id)
	return err
}

func (s *sessionStore) chatwootDedupExists(ctx context.Context, sessionID, sourceMessageID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM chatwoot_message_dedup WHERE session_id = $1 AND source_message_id = $2`,
		sessionID, sourceMessageID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *sessionStore) chatwootOutboxSent(ctx context.Context, sessionID, sourceMessageID string) (bool, error) {
	var status string
	err := s.db.QueryRowContext(ctx,
		`SELECT status FROM chatwoot_outbox WHERE session_id = $1 AND source_message_id = $2`,
		sessionID, sourceMessageID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return status == "sent", err
}

func (s *sessionStore) enqueueChatwootOutbox(ctx context.Context, it chatwootOutboxItem) error {
	now := time.Now().UnixMilli()
	if it.ID == "" {
		it.ID = newStoreID()
	}
	if it.Status == "" {
		it.Status = "pending"
	}
	if it.NextRetryAt == 0 {
		it.NextRetryAt = now
	}
	if it.CreatedAt == 0 {
		it.CreatedAt = now
	}
	if it.MediaBytes == nil {
		it.MediaBytes = []byte{}
	}
	it.UpdatedAt = now
	_, err := s.db.ExecContext(ctx, `INSERT INTO chatwoot_outbox (
			id, session_id, source_message_id, phone, source_id, chatwoot_account_id, chatwoot_inbox_id,
			chat_id, name, text, filename, mimetype, media_bytes, status, attempts, next_retry_at,
			last_error, chatwoot_conversation_id, chatwoot_message_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		ON CONFLICT(session_id, source_message_id) DO NOTHING`,
		it.ID, it.SessionID, it.SourceMessageID, it.Phone, it.SourceID, it.ChatwootAccountID, it.ChatwootInboxID,
		it.ChatID, it.Name, it.Text, it.Filename, it.Mimetype, it.MediaBytes, it.Status, it.Attempts, it.NextRetryAt,
		it.LastError, it.ChatwootConversationID, it.ChatwootMessageID, it.CreatedAt, it.UpdatedAt)
	return err
}

func (s *sessionStore) claimNextChatwootOutbox(ctx context.Context, now int64) (*chatwootOutboxItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `SELECT id, session_id, source_message_id, phone, source_id,
			chatwoot_account_id, chatwoot_inbox_id, chat_id, COALESCE(name, ''), COALESCE(text, ''),
			COALESCE(filename, ''), COALESCE(mimetype, ''), media_bytes, status, attempts, next_retry_at,
			COALESCE(last_error, ''), COALESCE(chatwoot_conversation_id, 0), COALESCE(chatwoot_message_id, 0),
			created_at, updated_at
		FROM chatwoot_outbox
		WHERE (status IN ('pending', 'failed') AND next_retry_at <= $1)
		   OR (status = 'processing' AND updated_at <= $2)
		ORDER BY created_at
		LIMIT 1`, now, now-5*60*1000)
	var it chatwootOutboxItem
	if err := row.Scan(&it.ID, &it.SessionID, &it.SourceMessageID, &it.Phone, &it.SourceID,
		&it.ChatwootAccountID, &it.ChatwootInboxID, &it.ChatID, &it.Name, &it.Text,
		&it.Filename, &it.Mimetype, &it.MediaBytes, &it.Status, &it.Attempts, &it.NextRetryAt,
		&it.LastError, &it.ChatwootConversationID, &it.ChatwootMessageID, &it.CreatedAt, &it.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE chatwoot_outbox SET status = 'processing', updated_at = $1
		WHERE id = $2 AND (status IN ('pending', 'failed') OR (status = 'processing' AND updated_at <= $3))`,
		now, it.ID, now-5*60*1000)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	it.Status = "processing"
	return &it, nil
}

func (s *sessionStore) markChatwootOutboxFailed(ctx context.Context, id string, attempts int, nextRetryAt int64, lastErr string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE chatwoot_outbox
		SET status = 'failed', attempts = $1, next_retry_at = $2, last_error = $3, updated_at = $4
		WHERE id = $5`, attempts, nextRetryAt, lastErr, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) markChatwootOutboxSent(ctx context.Context, it chatwootOutboxItem, conversationID, messageID int) error {
	now := time.Now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE chatwoot_outbox
		SET status = 'sent', chatwoot_conversation_id = $1, chatwoot_message_id = $2, updated_at = $3
		WHERE id = $4`, conversationID, messageID, now, it.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO chatwoot_message_dedup
		(id, session_id, source_message_id, phone, chatwoot_conversation_id, chatwoot_message_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT(session_id, source_message_id) DO NOTHING`,
		newStoreID(), it.SessionID, it.SourceMessageID, it.Phone, conversationID, messageID, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *sessionStore) findChatwootContactLink(ctx context.Context, sessionID, phone string, accountID, inboxID int) (*chatwootContactLink, error) {
	row := s.db.QueryRowContext(ctx, `SELECT session_id, phone, source_id, chatwoot_account_id, chatwoot_inbox_id,
			COALESCE(chatwoot_contact_id, 0), COALESCE(chatwoot_conversation_id, 0),
			COALESCE(last_conversation_status, ''), COALESCE(last_message_at, 0)
		FROM chatwoot_contact_links
		WHERE session_id = $1 AND phone = $2 AND chatwoot_account_id = $3 AND chatwoot_inbox_id = $4
		LIMIT 1`, sessionID, phone, accountID, inboxID)
	var link chatwootContactLink
	if err := row.Scan(&link.SessionID, &link.Phone, &link.SourceID, &link.ChatwootAccountID, &link.ChatwootInboxID,
		&link.ChatwootContactID, &link.ChatwootConversationID, &link.LastConversationStatus, &link.LastMessageAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &link, nil
}

func (s *sessionStore) upsertChatwootContactLink(ctx context.Context, link chatwootContactLink) error {
	now := time.Now().UnixMilli()
	if link.LastMessageAt == 0 {
		link.LastMessageAt = now
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO chatwoot_contact_links
		(id, session_id, phone, source_id, chatwoot_account_id, chatwoot_inbox_id,
		 chatwoot_contact_id, chatwoot_conversation_id, last_conversation_status, last_message_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT(session_id, phone, chatwoot_account_id, chatwoot_inbox_id) DO UPDATE SET
			source_id = excluded.source_id,
			chatwoot_contact_id = excluded.chatwoot_contact_id,
			chatwoot_conversation_id = excluded.chatwoot_conversation_id,
			last_conversation_status = excluded.last_conversation_status,
			last_message_at = excluded.last_message_at,
			updated_at = excluded.updated_at`,
		newStoreID(), link.SessionID, link.Phone, link.SourceID, link.ChatwootAccountID, link.ChatwootInboxID,
		link.ChatwootContactID, link.ChatwootConversationID, link.LastConversationStatus, link.LastMessageAt, now, now)
	return err
}

func (s *sessionStore) createCallRecording(ctx context.Context, rec callRecordingRow) error {
	now := time.Now().UnixMilli()
	if rec.ID == "" {
		rec.ID = newStoreID()
	}
	if rec.Status == "" {
		rec.Status = recordingStatusRecording
	}
	if rec.CreatedAt == 0 {
		rec.CreatedAt = now
	}
	rec.UpdatedAt = now
	_, err := s.db.ExecContext(ctx, `INSERT INTO call_recordings
		(id, session_id, call_id, direction, peer, phone, source_id, owner,
		 chatwoot_account_id, chatwoot_inbox_id, chatwoot_contact_id, chatwoot_conversation_id,
		 chatwoot_message_id, status, file_path, file_name, mime_type, size_bytes, duration_ms,
		 started_at, ended_at, error, upload_attempts, last_upload_attempt_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`,
		rec.ID, rec.SessionID, rec.CallID, rec.Direction, rec.Peer, rec.Phone, rec.SourceID, rec.Owner,
		rec.ChatwootAccountID, rec.ChatwootInboxID, rec.ChatwootContactID, rec.ChatwootConversationID,
		rec.ChatwootMessageID, rec.Status, rec.FilePath, rec.FileName, rec.MimeType, rec.SizeBytes, rec.DurationMs,
		rec.StartedAt, rec.EndedAt, rec.Error, rec.UploadAttempts, rec.LastUploadAttemptAt, rec.CreatedAt, rec.UpdatedAt)
	return err
}

func (s *sessionStore) getCallRecording(ctx context.Context, sessionID, callID string) (*callRecordingRow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, session_id, call_id, COALESCE(direction,''), COALESCE(peer,''), COALESCE(phone,''), COALESCE(source_id,''), COALESCE(owner,''),
			COALESCE(chatwoot_account_id,0), COALESCE(chatwoot_inbox_id,0), COALESCE(chatwoot_contact_id,0), COALESCE(chatwoot_conversation_id,0),
			COALESCE(chatwoot_message_id,0), status, COALESCE(file_path,''), COALESCE(file_name,''), COALESCE(mime_type,''), COALESCE(size_bytes,0),
			COALESCE(duration_ms,0), COALESCE(started_at,0), COALESCE(ended_at,0), COALESCE(error,''), COALESCE(upload_attempts,0),
			COALESCE(last_upload_attempt_at,0), created_at, updated_at
		FROM call_recordings
		WHERE session_id = $1 AND call_id = $2
		ORDER BY created_at DESC
		LIMIT 1`, sessionID, callID)
	rec, err := scanCallRecording(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return rec, err
}

func (s *sessionStore) getCallRecordingByID(ctx context.Context, id string) (*callRecordingRow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, session_id, call_id, COALESCE(direction,''), COALESCE(peer,''), COALESCE(phone,''), COALESCE(source_id,''), COALESCE(owner,''),
			COALESCE(chatwoot_account_id,0), COALESCE(chatwoot_inbox_id,0), COALESCE(chatwoot_contact_id,0), COALESCE(chatwoot_conversation_id,0),
			COALESCE(chatwoot_message_id,0), status, COALESCE(file_path,''), COALESCE(file_name,''), COALESCE(mime_type,''), COALESCE(size_bytes,0),
			COALESCE(duration_ms,0), COALESCE(started_at,0), COALESCE(ended_at,0), COALESCE(error,''), COALESCE(upload_attempts,0),
			COALESCE(last_upload_attempt_at,0), created_at, updated_at
		FROM call_recordings
		WHERE id = $1`, id)
	rec, err := scanCallRecording(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return rec, err
}

func (s *sessionStore) updateCallRecordingSaved(ctx context.Context, id string, result RecordingResult) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET
		status = $1, file_path = $2, file_name = $3, mime_type = $4, size_bytes = $5,
		duration_ms = $6, started_at = $7, ended_at = $8, updated_at = $9
		WHERE id = $10`,
		recordingStatusSaved, result.FilePath, result.FileName, result.MimeType, result.SizeBytes,
		result.DurationMs, result.StartedAt, result.EndedAt, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) updateCallRecordingFailed(ctx context.Context, id, status, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET status = $1, error = $2, updated_at = $3 WHERE id = $4`,
		status, msg, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) markCallRecordingUploading(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET status = $1, upload_attempts = upload_attempts + 1,
		last_upload_attempt_at = $2, updated_at = $2 WHERE id = $3`,
		recordingStatusUploading, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) markCallRecordingAttached(ctx context.Context, id string, conversationID, messageID int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET status = $1, chatwoot_conversation_id = $2,
		chatwoot_message_id = $3, error = '', updated_at = $4 WHERE id = $5`,
		recordingStatusAttached, conversationID, messageID, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) markCallRecordingUploadFailed(ctx context.Context, id, msg string) error {
	next := time.Now().Add(5 * time.Minute).UnixMilli()
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET status = $1, error = $2,
		last_upload_attempt_at = $3, updated_at = $4 WHERE id = $5`,
		recordingStatusUploadFailed, msg, next, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) updateCallRecordingChatwoot(ctx context.Context, id string, contactID, conversationID int, sourceID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET chatwoot_contact_id = $1,
		chatwoot_conversation_id = $2, source_id = $3, updated_at = $4 WHERE id = $5`,
		contactID, conversationID, sourceID, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) updateCallRecordingOwner(ctx context.Context, id, owner string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET owner = $1, updated_at = $2 WHERE id = $3`,
		owner, time.Now().UnixMilli(), id)
	return err
}

func (s *sessionStore) listCallRecordingsForUpload(ctx context.Context, now int64, limit int) ([]callRecordingRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, session_id, call_id, COALESCE(direction,''), COALESCE(peer,''), COALESCE(phone,''), COALESCE(source_id,''), COALESCE(owner,''),
			COALESCE(chatwoot_account_id,0), COALESCE(chatwoot_inbox_id,0), COALESCE(chatwoot_contact_id,0), COALESCE(chatwoot_conversation_id,0),
			COALESCE(chatwoot_message_id,0), status, COALESCE(file_path,''), COALESCE(file_name,''), COALESCE(mime_type,''), COALESCE(size_bytes,0),
			COALESCE(duration_ms,0), COALESCE(started_at,0), COALESCE(ended_at,0), COALESCE(error,''), COALESCE(upload_attempts,0),
			COALESCE(last_upload_attempt_at,0), created_at, updated_at
		FROM call_recordings
		WHERE status IN ('saved', 'upload_failed') AND file_path <> '' AND upload_attempts < 10
		  AND (last_upload_attempt_at IS NULL OR last_upload_attempt_at = 0 OR last_upload_attempt_at <= $1)
		ORDER BY updated_at
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []callRecordingRow
	for rows.Next() {
		rec, err := scanCallRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (s *sessionStore) listExpiredRecordingFiles(ctx context.Context, cutoff int64, limit int) ([]callRecordingRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, session_id, call_id, COALESCE(direction,''), COALESCE(peer,''), COALESCE(phone,''), COALESCE(source_id,''), COALESCE(owner,''),
			COALESCE(chatwoot_account_id,0), COALESCE(chatwoot_inbox_id,0), COALESCE(chatwoot_contact_id,0), COALESCE(chatwoot_conversation_id,0),
			COALESCE(chatwoot_message_id,0), status, COALESCE(file_path,''), COALESCE(file_name,''), COALESCE(mime_type,''), COALESCE(size_bytes,0),
			COALESCE(duration_ms,0), COALESCE(started_at,0), COALESCE(ended_at,0), COALESCE(error,''), COALESCE(upload_attempts,0),
			COALESCE(last_upload_attempt_at,0), created_at, updated_at
		FROM call_recordings
		WHERE status = 'attached' AND file_path <> '' AND ended_at > 0 AND ended_at <= $1
		ORDER BY ended_at
		LIMIT $2`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []callRecordingRow
	for rows.Next() {
		rec, err := scanCallRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (s *sessionStore) markRecordingFileRemoved(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE call_recordings SET file_path = '', updated_at = $1 WHERE id = $2`,
		time.Now().UnixMilli(), id)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanCallRecording(row scanner) (*callRecordingRow, error) {
	var rec callRecordingRow
	err := row.Scan(&rec.ID, &rec.SessionID, &rec.CallID, &rec.Direction, &rec.Peer, &rec.Phone, &rec.SourceID, &rec.Owner,
		&rec.ChatwootAccountID, &rec.ChatwootInboxID, &rec.ChatwootContactID, &rec.ChatwootConversationID,
		&rec.ChatwootMessageID, &rec.Status, &rec.FilePath, &rec.FileName, &rec.MimeType, &rec.SizeBytes,
		&rec.DurationMs, &rec.StartedAt, &rec.EndedAt, &rec.Error, &rec.UploadAttempts, &rec.LastUploadAttemptAt,
		&rec.CreatedAt, &rec.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &rec, nil
}
