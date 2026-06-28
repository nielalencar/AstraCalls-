package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// Integração com Chatwoot (canal API), inspirada no app chatwoot do WAHA.
// Mapeia o contato Chatwoot <-> chat do WhatsApp via custom attribute.

const cwChatIDAttr = "wacalls_chat_id"
const cwDefaultSourceIDStrategy = "astra_session_phone"

type ChatwootConfig struct {
	URL             string `json:"url"`
	AccountID       int    `json:"account_id"`
	AccountToken    string `json:"account_token"`
	InboxID         int    `json:"inbox_id"`
	InboxIdentifier string `json:"inbox_identifier"`
}

func (c ChatwootConfig) valid() bool {
	return c.URL != "" && c.AccountID != 0 && c.AccountToken != "" && c.InboxID != 0
}

func (c ChatwootConfig) base() string {
	return strings.TrimRight(c.URL, "/") + "/api/v1/accounts/" + strconv.Itoa(c.AccountID)
}

var cwHTTP = &http.Client{Timeout: 30 * time.Second}
var errChatwootDuplicate = errors.New("chatwoot duplicate message")

type chatwootRuntimeConfig struct {
	ReopenResolved  bool
	SourceStrategy  string
	ReopenWindowDays int
	UseLocalCache   bool
	DedupEnabled    bool
}

type chatwootConversation struct {
	ID        int
	InboxID   int
	ContactID int
	SourceID  string
	Status    string
	UpdatedAt int64
	Created   bool
}

func chatwootRuntime() chatwootRuntimeConfig {
	return chatwootRuntimeConfig{
		ReopenResolved:  envBool("CHATWOOT_REOPEN_RESOLVED_CONVERSATION", true),
		SourceStrategy:  envStr("CHATWOOT_SOURCE_ID_STRATEGY", cwDefaultSourceIDStrategy),
		ReopenWindowDays: envInt("CHATWOOT_REOPEN_WINDOW_DAYS", 0),
		UseLocalCache:   envBool("CHATWOOT_USE_LOCAL_CONVERSATION_CACHE", true),
		DedupEnabled:    envBool("CHATWOOT_DEDUP_ENABLED", true),
	}
}

func envBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "":
		return def
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func normalizeChatwootPhone(phone string) string {
	phone = strings.TrimSpace(strings.TrimPrefix(phone, "+"))
	var b strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func buildChatwootSourceID(sessionID, phone, strategy string) string {
	phone = normalizeChatwootPhone(phone)
	if strategy == "phone_only" {
		return "whatsapp:" + phone
	}
	return "astra:" + sessionID + ":" + phone
}

// cwReq faz uma chamada JSON na Application API do Chatwoot.
func (c ChatwootConfig) req(method, path string, body any) (map[string]any, int, error) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base()+path, rdr)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("api_access_token", c.AccountToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := cwHTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return out, resp.StatusCode, nil
}

// ---------- WhatsApp -> Chatwoot (entrada) ----------

// realPhone devolve o telefone real (PN). Se o JID for um LID, tenta converter
// via store; senão devolve o próprio user.
func (s *Session) realPhone(jid types.JID) string {
	if jid.User == "" {
		return ""
	}
	if jid.Server == types.DefaultUserServer {
		return jid.User
	}
	if pn, err := s.client.Store.LIDs.GetPNForLID(context.Background(), jid); err == nil && pn.User != "" {
		return pn.User
	}
	return jid.User
}

func (s *Session) chatwootPushIncoming(evt *events.Message) {
	cfg := s.getChatwoot()
	if !cfg.valid() || evt.Info.IsFromMe || evt.Info.IsGroup {
		return
	}
	// telefone real (PN), nunca o LID
	chat := evt.Info.Chat
	phone := chat.User
	if chat.Server != types.DefaultUserServer {
		if evt.Info.SenderAlt.Server == types.DefaultUserServer && evt.Info.SenderAlt.User != "" {
			phone = evt.Info.SenderAlt.User
		} else {
			phone = s.realPhone(chat)
		}
	}
	phone = normalizeChatwootPhone(phone)
	if phone == "" || evt.Info.ID == "" {
		return
	}
	chatID := phone + "@" + types.DefaultUserServer
	name := evt.Info.PushName
	if name == "" {
		name = phone
	}

	rt := chatwootRuntime()
	sourceID := buildChatwootSourceID(s.id, phone, rt.SourceStrategy)
	s.log.Info("chatwoot_source_id_built", "phone", phone, "source_id", sourceID, "strategy", rt.SourceStrategy)

	if rt.DedupEnabled {
		if ok, err := s.mgr.store.chatwootDedupExists(context.Background(), s.id, evt.Info.ID); err == nil && ok {
			s.log.Info("chatwoot_message_duplicate_ignored", "source_message_id", evt.Info.ID, "phone", phone)
			return
		}
		if ok, err := s.mgr.store.chatwootOutboxSent(context.Background(), s.id, evt.Info.ID); err == nil && ok {
			s.log.Info("chatwoot_message_duplicate_ignored", "source_message_id", evt.Info.ID, "phone", phone)
			return
		}
	}

	text := messageText(evt.Message)
	filename, mimetype := "", ""
	var mediaBytes []byte
	if dl := downloadableOf(evt.Message); dl != nil {
		data, derr := s.client.Download(context.Background(), dl)
		if derr != nil {
			s.log.Error("chatwoot: download media before outbox failed", "err", derr, "source_message_id", evt.Info.ID)
			return
		}
		filename, mimetype = mediaMeta(evt.Message)
		mediaBytes = data
	}
	if strings.TrimSpace(text) == "" && len(mediaBytes) == 0 {
		return
	}

	err := s.mgr.store.enqueueChatwootOutbox(context.Background(), chatwootOutboxItem{
		SessionID:         s.id,
		SourceMessageID:   evt.Info.ID,
		Phone:             phone,
		SourceID:          sourceID,
		ChatwootAccountID: cfg.AccountID,
		ChatwootInboxID:   cfg.InboxID,
		ChatID:            chatID,
		Name:              name,
		Text:              text,
		Filename:          filename,
		Mimetype:          mimetype,
		MediaBytes:        mediaBytes,
	})
	if err != nil {
		s.log.Error("chatwoot: enqueue outbox failed", "err", err, "source_message_id", evt.Info.ID)
	}
}

// avatarSynced evita re-sincronizar a foto a cada mensagem (1x por contato/processo).
var avatarSynced sync.Map

// ensureContact acha (por telefone) ou cria o contato e garante o source_id da inbox.
func (c ChatwootConfig) ensureContact(chatID, phone, name, avatarURL, wantedSourceID string) (contactID int, sourceID string, err error) {
	// procura por telefone
	if res, code, e := c.req(http.MethodGet, "/contacts/search?q="+url.QueryEscape(phone), nil); e == nil && code == 200 {
		for _, it := range asList(res["payload"]) {
			m := asMap(it)
			if id := asInt(m["id"]); id != 0 {
				c.syncAvatar(id, avatarURL)
				if sid := sourceIDForInbox(m, c.InboxID); sid != "" {
					return id, sid, nil
				}
				// achou contato mas sem source_id p/ esta inbox -> cria contact_inbox
				sid, e2 := c.ensureContactInbox(id, wantedSourceID)
				return id, sid, e2
			}
		}
	}
	// cria contato
	body := map[string]any{
		"inbox_id":     c.InboxID,
		"name":         name,
		"phone_number": "+" + phone,
		"identifier":   chatID,
		"source_id":    wantedSourceID,
		"custom_attributes": map[string]any{
			cwChatIDAttr:          chatID,
			"source":              "astracalls",
			"astracalls_source_id": wantedSourceID,
		},
	}
	if avatarURL != "" {
		body["avatar_url"] = avatarURL
	}
	res, code, e := c.req(http.MethodPost, "/contacts", body)
	if e != nil {
		return 0, "", e
	}
	if code >= 300 {
		return 0, "", fmt.Errorf("create contact http %d", code)
	}
	contact := asMap(asMap(res["payload"])["contact"])
	if len(contact) == 0 {
		contact = asMap(res["payload"])
	}
	if len(contact) == 0 {
		contact = res
	}
	id := asInt(contact["id"])
	if id == 0 {
		return 0, "", fmt.Errorf("create contact response missing id")
	}
	if avatarURL != "" {
		avatarSynced.Store(fmt.Sprintf("%d:%d", c.AccountID, id), true)
	}
	sid := sourceIDForInbox(contact, c.InboxID)
	if sid == "" {
		sid, _ = c.ensureContactInbox(id, wantedSourceID)
	}
	return id, sid, nil
}

// syncAvatar atualiza a foto do contato existente (uma vez por processo).
func (c ChatwootConfig) syncAvatar(contactID int, avatarURL string) {
	if avatarURL == "" {
		return
	}
	key := fmt.Sprintf("%d:%d", c.AccountID, contactID)
	if _, done := avatarSynced.LoadOrStore(key, true); done {
		return
	}
	_, _, _ = c.req(http.MethodPut, fmt.Sprintf("/contacts/%d", contactID), map[string]any{"avatar_url": avatarURL})
}

func (c ChatwootConfig) ensureContactInbox(contactID int, sourceID string) (string, error) {
	body := map[string]any{"inbox_id": c.InboxID, "source_id": sourceID}
	res, _, e := c.req(http.MethodPost, fmt.Sprintf("/contacts/%d/contact_inboxes", contactID), body)
	if e != nil {
		return "", e
	}
	if sid := asStr(res["source_id"]); sid != "" {
		return sid, nil
	}
	if sid := asStr(asMap(res["payload"])["source_id"]); sid != "" {
		return sid, nil
	}
	return sourceID, nil
}

// ensureConversation reutiliza uma conversa aberta da inbox ou cria uma nova.
func (c ChatwootConfig) ensureConversation(contactID int, sourceID, phone string) (chatwootConversation, error) {
	if res, code, e := c.req(http.MethodGet, fmt.Sprintf("/contacts/%d/conversations", contactID), nil); e == nil && code == 200 {
		var best chatwootConversation
		for _, it := range asList(res["payload"]) {
			m := asMap(it)
			if asInt(m["inbox_id"]) == c.InboxID {
				conv := conversationFromMap(m)
				if conv.SourceID != "" && sourceID != "" && conv.SourceID != sourceID {
					continue
				}
				if conv.UpdatedAt >= best.UpdatedAt {
					best = conv
				}
			}
		}
		if best.ID != 0 {
			return best, nil
		}
	}
	body := map[string]any{
		"source_id": sourceID, "inbox_id": c.InboxID, "contact_id": contactID, "status": "open",
		"custom_attributes": map[string]any{
			"whatsapp_phone":       phone,
			"astracalls_source_id": sourceID,
			"source_id_strategy":   chatwootRuntime().SourceStrategy,
		},
	}
	res, code, e := c.req(http.MethodPost, "/conversations", body)
	if e != nil {
		return chatwootConversation{}, e
	}
	if code >= 300 {
		return chatwootConversation{}, fmt.Errorf("create conversation http %d", code)
	}
	conv := conversationFromMap(res)
	if conv.ID == 0 {
		conv.ID = asInt(res["id"])
	}
	conv.InboxID = firstNonZero(conv.InboxID, c.InboxID)
	conv.ContactID = firstNonZero(conv.ContactID, contactID)
	conv.SourceID = firstNonEmpty(conv.SourceID, sourceID)
	conv.Status = firstNonEmpty(conv.Status, "open")
	conv.Created = true
	return conv, nil
}

func (c ChatwootConfig) conversationDetails(convID int) (chatwootConversation, error) {
	res, code, e := c.req(http.MethodGet, fmt.Sprintf("/conversations/%d", convID), nil)
	if e != nil {
		return chatwootConversation{}, e
	}
	if code >= 300 {
		return chatwootConversation{}, fmt.Errorf("conversation details http %d", code)
	}
	return conversationFromMap(res), nil
}

func (c ChatwootConfig) reopenConversation(convID int) error {
	_, code, e := c.req(http.MethodPost, fmt.Sprintf("/conversations/%d/toggle_status", convID), map[string]any{"status": "open"})
	if e != nil {
		return e
	}
	if code >= 300 {
		return fmt.Errorf("toggle status http %d", code)
	}
	return nil
}

func (c ChatwootConfig) postText(convID int, content string) (int, error) {
	res, code, e := c.req(http.MethodPost, fmt.Sprintf("/conversations/%d/messages", convID), map[string]any{
		"content": content, "message_type": "incoming", "private": false, "content_type": "text", "content_attributes": map[string]any{},
	})
	if e != nil {
		return 0, e
	}
	if code >= 300 {
		return 0, fmt.Errorf("post message http %d", code)
	}
	return asInt(res["id"]), nil
}

// postAttachment sobe a mídia como anexo (multipart) numa mensagem incoming.
func (c ChatwootConfig) postAttachment(convID int, content, filename, mime string, data []byte) (int, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("message_type", "incoming")
	_ = mw.WriteField("private", "false")
	if content != "" {
		_ = mw.WriteField("content", content)
	}
	h := make(map[string][]string)
	h["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="attachments[]"; filename=%q`, filename)}
	h["Content-Type"] = []string{mime}
	pw, _ := mw.CreatePart(h)
	_, _ = pw.Write(data)
	mw.Close()

	url := c.base() + fmt.Sprintf("/conversations/%d/messages", convID)
	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("api_access_token", c.AccountToken)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := cwHTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	dataResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("post attachment http %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.Unmarshal(dataResp, &out)
	return asInt(out["id"]), nil
}

func (c ChatwootConfig) postRecordingAttachment(convID int, content, filename, mime, filePath string, private bool) (int, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("message_type", "outgoing")
	_ = mw.WriteField("private", fmt.Sprintf("%t", private))
	_ = mw.WriteField("content_type", "text")
	if content != "" {
		_ = mw.WriteField("content", content)
	}
	h := make(map[string][]string)
	h["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="attachments[]"; filename=%q`, filename)}
	h["Content-Type"] = []string{mime}
	pw, _ := mw.CreatePart(h)
	_, _ = pw.Write(data)
	mw.Close()

	url := c.base() + fmt.Sprintf("/conversations/%d/messages", convID)
	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("api_access_token", c.AccountToken)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := cwHTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	dataResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("post recording http %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.Unmarshal(dataResp, &out)
	return asInt(out["id"]), nil
}

func (m *SessionManager) processChatwootOutboxItem(it chatwootOutboxItem) {
	sess, ok := m.Get(it.SessionID)
	if !ok {
		m.rescheduleChatwootOutbox(it, fmt.Errorf("session not loaded"))
		return
	}
	cfg := sess.getChatwoot()
	if !cfg.valid() || cfg.AccountID != it.ChatwootAccountID || cfg.InboxID != it.ChatwootInboxID {
		m.rescheduleChatwootOutbox(it, fmt.Errorf("chatwoot config not available for session"))
		return
	}
	unlock := m.chatwootLock(fmt.Sprintf("%s:%s:%d:%d", it.SessionID, it.Phone, it.ChatwootAccountID, it.ChatwootInboxID))
	defer unlock()

	convID, _, err := m.ensureChatwootConversationForOutbox(sess, cfg, it)
	if err != nil {
		if errors.Is(err, errChatwootDuplicate) {
			return
		}
		m.rescheduleChatwootOutbox(it, err)
		return
	}

	msgID := 0
	if len(it.MediaBytes) > 0 {
		msgID, err = cfg.postAttachment(convID, it.Text, it.Filename, it.Mimetype, it.MediaBytes)
	} else {
		msgID, err = cfg.postText(convID, it.Text)
	}
	if err != nil {
		m.rescheduleChatwootOutbox(it, err)
		return
	}
	if err := m.store.markChatwootOutboxSent(m.appCtx, it, convID, msgID); err != nil {
		m.log.Error("chatwoot: mark outbox sent failed", "err", err, "source_message_id", it.SourceMessageID)
		return
	}
	m.log.Info("chatwoot_message_created", "session_id", it.SessionID, "phone", it.Phone, "conversation_id", convID, "message_id", msgID)
}

func (m *SessionManager) ensureChatwootConversationForOutbox(sess *Session, cfg ChatwootConfig, it chatwootOutboxItem) (int, string, error) {
	rt := chatwootRuntime()
	if rt.DedupEnabled {
		if ok, err := m.store.chatwootDedupExists(m.appCtx, it.SessionID, it.SourceMessageID); err != nil {
			return 0, "", err
		} else if ok {
			m.log.Info("chatwoot_message_duplicate_ignored", "source_message_id", it.SourceMessageID, "phone", it.Phone)
			if err := m.store.markChatwootOutboxSent(m.appCtx, it, it.ChatwootConversationID, it.ChatwootMessageID); err != nil {
				return 0, "", err
			}
			return 0, "", errChatwootDuplicate
		}
	}

	var link *chatwootContactLink
	var err error
	if rt.UseLocalCache {
		link, err = m.store.findChatwootContactLink(m.appCtx, it.SessionID, it.Phone, it.ChatwootAccountID, it.ChatwootInboxID)
		if err != nil {
			return 0, "", err
		}
		if link != nil {
			m.log.Info("chatwoot_contact_link_found", "session_id", it.SessionID, "phone", it.Phone, "conversation_id", link.ChatwootConversationID)
		}
	}

	contactID := 0
	sourceID := it.SourceID
	if link != nil && link.ChatwootContactID != 0 {
		contactID = link.ChatwootContactID
		sourceID = firstNonEmpty(link.SourceID, sourceID)
	} else {
		avatar := ""
		if sess.client != nil {
			if pp, perr := sess.client.GetProfilePictureInfo(context.Background(), types.NewJID(it.Phone, types.DefaultUserServer), nil); perr == nil && pp != nil {
				avatar = pp.URL
			}
		}
		contactID, sourceID, err = cfg.ensureContact(it.ChatID, it.Phone, it.Name, avatar, it.SourceID)
		if err != nil {
			return 0, "", fmt.Errorf("ensure contact: %w", err)
		}
		if sourceID != it.SourceID {
			m.log.Warn("chatwoot: preserving legacy contact_inbox source_id", "wanted", it.SourceID, "actual", sourceID, "phone", it.Phone)
		}
	}

	conv := chatwootConversation{}
	if link != nil && link.ChatwootConversationID != 0 {
		conv, err = cfg.conversationDetails(link.ChatwootConversationID)
		if err != nil {
			m.log.Warn("chatwoot: cached conversation lookup failed; falling back to contact conversations", "conversation_id", link.ChatwootConversationID, "err", err)
		}
	}
	if conv.ID == 0 {
		conv, err = cfg.ensureConversation(contactID, sourceID, it.Phone)
		if err != nil {
			return 0, "", fmt.Errorf("ensure conversation: %w", err)
		}
		if conv.ID != 0 {
			if conv.Created {
				m.log.Info("chatwoot_conversation_created", "session_id", it.SessionID, "phone", it.Phone, "source_id", sourceID, "conversation_id", conv.ID)
			} else {
				m.log.Info("chatwoot_conversation_found", "session_id", it.SessionID, "phone", it.Phone, "source_id", sourceID, "conversation_id", conv.ID)
			}
		}
	}
	if conv.ID == 0 {
		return 0, "", fmt.Errorf("chatwoot conversation id missing")
	}
	if conv.Status == "" {
		conv.Status = "open"
	}
	if conv.Status == "resolved" && rt.ReopenResolved && withinChatwootReopenWindow(conv, rt.ReopenWindowDays) {
		if err := cfg.reopenConversation(conv.ID); err != nil {
			m.log.Error("chatwoot_reopen_failed", "session_id", it.SessionID, "phone", it.Phone, "conversation_id", conv.ID, "err", err)
			return 0, "", err
		}
		conv.Status = "open"
		m.log.Info("chatwoot_conversation_reopened", "session_id", it.SessionID, "phone", it.Phone, "source_id", sourceID, "conversation_id", conv.ID)
	}
	if err := m.store.upsertChatwootContactLink(m.appCtx, chatwootContactLink{
		SessionID: it.SessionID, Phone: it.Phone, SourceID: sourceID,
		ChatwootAccountID: it.ChatwootAccountID, ChatwootInboxID: it.ChatwootInboxID,
		ChatwootContactID: contactID, ChatwootConversationID: conv.ID,
		LastConversationStatus: conv.Status, LastMessageAt: time.Now().UnixMilli(),
	}); err != nil {
		return 0, "", err
	}
	if link == nil {
		m.log.Info("chatwoot_contact_link_created", "session_id", it.SessionID, "phone", it.Phone, "source_id", sourceID, "conversation_id", conv.ID)
	}
	return conv.ID, conv.Status, nil
}

func (m *SessionManager) rescheduleChatwootOutbox(it chatwootOutboxItem, err error) {
	attempts := it.Attempts + 1
	delay := time.Duration(1<<intMin(attempts, 8)) * time.Second
	next := time.Now().Add(delay).UnixMilli()
	msg := err.Error()
	if len(msg) > 500 {
		msg = msg[:500]
	}
	if e := m.store.markChatwootOutboxFailed(m.appCtx, it.ID, attempts, next, msg); e != nil {
		m.log.Error("chatwoot: mark outbox failed failed", "err", e, "source_message_id", it.SourceMessageID)
		return
	}
	m.log.Warn("chatwoot_outbox_retry_scheduled", "session_id", it.SessionID, "phone", it.Phone, "source_message_id", it.SourceMessageID, "attempts", attempts, "err", msg)
}

func (m *SessionManager) uploadCallRecording(rec callRecordingRow) {
	if rec.Status == recordingStatusAttached || rec.FilePath == "" {
		return
	}
	sess, ok := m.Get(rec.SessionID)
	if !ok {
		_ = m.store.markCallRecordingUploadFailed(m.appCtx, rec.ID, "session not loaded")
		return
	}
	cfg := sess.getChatwoot()
	if !cfg.valid() {
		_ = m.store.markCallRecordingUploadFailed(m.appCtx, rec.ID, "chatwoot config not available")
		return
	}
	if rec.ChatwootAccountID != 0 && rec.ChatwootAccountID != cfg.AccountID {
		_ = m.store.markCallRecordingUploadFailed(m.appCtx, rec.ID, "recording belongs to another chatwoot account")
		return
	}
	if rec.ChatwootInboxID != 0 && rec.ChatwootInboxID != cfg.InboxID {
		_ = m.store.markCallRecordingUploadFailed(m.appCtx, rec.ID, "recording belongs to another chatwoot inbox")
		return
	}
	m.log.Info("recording_upload_started", "recording_id", rec.ID, "call_id", rec.CallID)
	if err := m.store.markCallRecordingUploading(m.appCtx, rec.ID); err != nil {
		m.log.Error("recording upload mark failed", "recording_id", rec.ID, "err", err)
		return
	}
	convID, contactID, sourceID, err := m.resolveRecordingConversation(sess, cfg, rec)
	if err != nil {
		_ = m.store.markCallRecordingUploadFailed(m.appCtx, rec.ID, err.Error())
		m.log.Warn("recording_upload_failed", "recording_id", rec.ID, "call_id", rec.CallID, "err", err)
		return
	}
	content := recordingNoteContent(rec)
	msgID, err := cfg.postRecordingAttachment(convID, content, rec.FileName, firstNonEmpty(rec.MimeType, "audio/wav"), rec.FilePath, recordingConfigFromEnv().PrivateNote)
	if err != nil {
		_ = m.store.markCallRecordingUploadFailed(m.appCtx, rec.ID, err.Error())
		m.log.Warn("recording_upload_failed", "recording_id", rec.ID, "call_id", rec.CallID, "err", err)
		return
	}
	_ = m.store.updateCallRecordingChatwoot(m.appCtx, rec.ID, contactID, convID, sourceID)
	if err := m.store.markCallRecordingAttached(m.appCtx, rec.ID, convID, msgID); err != nil {
		m.log.Error("recording upload attached persistence failed", "recording_id", rec.ID, "err", err)
		return
	}
	m.log.Info("recording_upload_succeeded", "recording_id", rec.ID, "call_id", rec.CallID, "conversation_id", convID, "message_id", msgID)
}

func (m *SessionManager) resolveRecordingConversation(sess *Session, cfg ChatwootConfig, rec callRecordingRow) (int, int, string, error) {
	if rec.ChatwootConversationID != 0 {
		conv, err := cfg.conversationDetails(rec.ChatwootConversationID)
		if err == nil && conv.ID != 0 {
			return conv.ID, firstNonZero(rec.ChatwootContactID, conv.ContactID), firstNonEmpty(rec.SourceID, conv.SourceID), nil
		}
	}
	phone := normalizeChatwootPhone(rec.Phone)
	if phone == "" {
		if jid, err := types.ParseJID(rec.Peer); err == nil {
			phone = normalizeChatwootPhone(jid.User)
		}
	}
	if phone == "" {
		return 0, 0, "", fmt.Errorf("recording has no phone")
	}
	sourceID := firstNonEmpty(rec.SourceID, buildChatwootSourceID(rec.SessionID, phone, chatwootRuntime().SourceStrategy))
	if link, err := m.store.findChatwootContactLink(m.appCtx, rec.SessionID, phone, cfg.AccountID, cfg.InboxID); err == nil && link != nil {
		if link.ChatwootConversationID != 0 {
			return link.ChatwootConversationID, link.ChatwootContactID, firstNonEmpty(link.SourceID, sourceID), nil
		}
	}
	chatID := phone + "@" + types.DefaultUserServer
	contactID, actualSourceID, err := cfg.ensureContact(chatID, phone, phone, "", sourceID)
	if err != nil {
		return 0, 0, "", err
	}
	conv, err := cfg.ensureConversation(contactID, actualSourceID, phone)
	if err != nil {
		return 0, 0, "", err
	}
	if conv.ID == 0 {
		return 0, 0, "", fmt.Errorf("chatwoot conversation id missing")
	}
	_ = m.store.upsertChatwootContactLink(m.appCtx, chatwootContactLink{
		SessionID: rec.SessionID, Phone: phone, SourceID: actualSourceID,
		ChatwootAccountID: cfg.AccountID, ChatwootInboxID: cfg.InboxID,
		ChatwootContactID: contactID, ChatwootConversationID: conv.ID,
		LastConversationStatus: firstNonEmpty(conv.Status, "open"), LastMessageAt: time.Now().UnixMilli(),
	})
	return conv.ID, contactID, actualSourceID, nil
}

func recordingNoteContent(rec callRecordingRow) string {
	dir := rec.Direction
	if dir == "outbound" {
		dir = "Realizada"
	} else if dir == "inbound" {
		dir = "Recebida"
	}
	return fmt.Sprintf("Gravacao de chamada WhatsApp\n\nDirecao: %s\nDuracao: %s\nOperador: %s\nSessao: %s\nCall ID: %s\n\nArquivo anexado automaticamente.",
		firstNonEmpty(dir, rec.Direction), formatDurationMS(rec.DurationMs), firstNonEmpty(rec.Owner, "-"), rec.SessionID, rec.CallID)
}

func formatDurationMS(ms int64) string {
	if ms <= 0 {
		return "00:00"
	}
	total := ms / 1000
	return fmt.Sprintf("%02d:%02d", total/60, total%60)
}

// ---------- Chatwoot -> WhatsApp (saída via webhook) ----------

func (s *server) handleChatwootWebhook(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	// só processa mensagens de saída do agente
	if asStr(body["event"]) != "message_created" || asStr(body["message_type"]) != "outgoing" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if b, ok := body["private"].(bool); ok && b {
		w.WriteHeader(http.StatusOK)
		return
	}

	chatID := chatIDFromWebhook(body)
	if chatID == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	jid, err := resolveRecipient(chatID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	content := asStr(body["content"])
	attachments := asList(body["attachments"])
	ctx := r.Context()

	// texto (só envia separado se não houver exatamente 1 anexo, igual ao WAHA)
	if strings.TrimSpace(content) != "" && len(attachments) != 1 {
		_, _ = sess.client.SendMessage(ctx, jid, &waE2E.Message{Conversation: proto.String(content)})
	}
	// anexos
	for _, it := range attachments {
		a := asMap(it)
		url := asStr(a["data_url"])
		if url == "" {
			continue
		}
		caption := ""
		if len(attachments) == 1 {
			caption = content
		}
		if err := sess.sendChatwootFile(ctx, jid, asStr(a["file_type"]), url, caption); err != nil {
			s.log.Error("chatwoot->wa: send file failed", "err", err)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// sendChatwootFile baixa o anexo do Chatwoot e envia pelo WhatsApp.
func (s *Session) sendChatwootFile(ctx context.Context, jid types.JID, fileType, url, caption string) error {
	data, err := fetchMedia("", url)
	if err != nil {
		return err
	}
	filename := url[strings.LastIndex(url, "/")+1:]
	switch fileType {
	case "image":
		up, e := s.client.Upload(ctx, data, whatsmeow.MediaImage)
		if e != nil {
			return e
		}
		_, e = s.client.SendMessage(ctx, jid, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			Caption: proto.String(caption), Mimetype: proto.String("image/jpeg"),
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		}})
		return e
	case "audio":
		ogg, seconds, waveform, terr := transcodeVoice(data)
		if terr != nil {
			ogg = data // fallback: envia o original
		}
		up, e := s.client.Upload(ctx, ogg, whatsmeow.MediaAudio)
		if e != nil {
			return e
		}
		am := &waE2E.AudioMessage{
			Mimetype: proto.String("audio/ogg; codecs=opus"), PTT: proto.Bool(true),
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		}
		if terr == nil {
			am.Seconds = proto.Uint32(seconds)
			am.Waveform = waveform
		}
		_, e = s.client.SendMessage(ctx, jid, &waE2E.Message{AudioMessage: am})
		return e
	case "video":
		up, e := s.client.Upload(ctx, data, whatsmeow.MediaVideo)
		if e != nil {
			return e
		}
		_, e = s.client.SendMessage(ctx, jid, &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			Caption: proto.String(caption), Mimetype: proto.String("video/mp4"),
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		}})
		return e
	default:
		up, e := s.client.Upload(ctx, data, whatsmeow.MediaDocument)
		if e != nil {
			return e
		}
		_, e = s.client.SendMessage(ctx, jid, &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			FileName: proto.String(filename), Title: proto.String(filename),
			Mimetype: proto.String("application/octet-stream"),
			URL:      &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		}})
		return e
	}
}

// transcodeVoice converte um áudio qualquer em OGG/Opus (nota de voz) e calcula
// a duração e o waveform (64 bytes) p/ o WhatsApp mostrar as ondinhas e o tempo.
func transcodeVoice(input []byte) (ogg []byte, seconds uint32, waveform []byte, err error) {
	tmp, err := os.CreateTemp("", "cwaud-*")
	if err != nil {
		return nil, 0, nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(input); err != nil {
		tmp.Close()
		return nil, 0, nil, err
	}
	tmp.Close()

	var oggBuf bytes.Buffer
	c1 := exec.Command("ffmpeg", "-y", "-i", tmp.Name(), "-ac", "1", "-ar", "48000", "-c:a", "libopus", "-b:a", "32k", "-f", "ogg", "pipe:1")
	c1.Stdout = &oggBuf
	if err = c1.Run(); err != nil {
		return nil, 0, nil, err
	}

	var pcmBuf bytes.Buffer
	c2 := exec.Command("ffmpeg", "-y", "-i", tmp.Name(), "-ac", "1", "-ar", "8000", "-f", "s16le", "pipe:1")
	c2.Stdout = &pcmBuf
	if err = c2.Run(); err != nil {
		return oggBuf.Bytes(), 0, nil, err
	}
	pcm := pcmBuf.Bytes()
	seconds = uint32(len(pcm) / 2 / 8000)
	return oggBuf.Bytes(), seconds, computeWaveform(pcm), nil
}

func computeWaveform(pcm []byte) []byte {
	const buckets = 64
	out := make([]byte, buckets)
	n := len(pcm) / 2
	if n == 0 {
		return out
	}
	per := n / buckets
	if per < 1 {
		per = 1
	}
	rms := make([]float64, buckets)
	var maxv float64
	for b := 0; b < buckets; b++ {
		start := b * per
		if start >= n {
			break
		}
		end := start + per
		if end > n {
			end = n
		}
		var sum float64
		for i := start; i < end; i++ {
			s := int16(binary.LittleEndian.Uint16(pcm[i*2:]))
			v := float64(s) / 32768.0
			sum += v * v
		}
		r := math.Sqrt(sum / float64(end-start))
		rms[b] = r
		if r > maxv {
			maxv = r
		}
	}
	if maxv > 0 {
		for b := 0; b < buckets; b++ {
			out[b] = byte(rms[b] / maxv * 100)
		}
	}
	return out
}

// extrai o chat id do WhatsApp a partir do payload do webhook do Chatwoot
func chatIDFromWebhook(body map[string]any) string {
	sender := asMap(asMap(asMap(body["conversation"])["meta"])["sender"])
	if ca := asMap(sender["custom_attributes"]); ca != nil {
		if v := asStr(ca[cwChatIDAttr]); v != "" {
			return v
		}
	}
	if ph := asStr(sender["phone_number"]); ph != "" {
		return strings.TrimPrefix(ph, "+")
	}
	if id := asStr(sender["identifier"]); id != "" {
		return id
	}
	return ""
}

// handleChatwootResolve: dado account_id + conversation_id, descobre a sessão
// ligada e o telefone do contato (consultando a API do Chatwoot). Usado pelo widget.
func (s *server) handleChatwootResolve(w http.ResponseWriter, r *http.Request) {
	accountID := asInt(r.URL.Query().Get("account_id"))
	convID := r.URL.Query().Get("conversation_id")
	if accountID == 0 || convID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "account_id and conversation_id required"})
		return
	}
	s.log.Info("chatwoot resolve", "account_id", accountID, "conversation_id", convID)
	// Qualquer sessão da conta serve só para consultar a conversa (mesmo token de conta).
	probe := s.sessions.sessionForChatwootAccount(accountID)
	if probe == nil {
		s.log.Warn("chatwoot resolve: no session for account", "account_id", accountID)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no session linked to this chatwoot account"})
		return
	}
	res, code, err := probe.getChatwoot().req(http.MethodGet, "/conversations/"+convID, nil)
	if err != nil || code >= 300 {
		s.log.Error("chatwoot resolve: lookup failed", "code", code, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "chatwoot lookup failed"})
		return
	}
	// Amarra empresa + caixa: a sessão tem que ser a da inbox desta conversa.
	inboxID := asInt(res["inbox_id"])
	sess := s.sessions.sessionForChatwootInbox(accountID, inboxID)
	if sess == nil {
		s.log.Warn("chatwoot resolve: no session for inbox", "account_id", accountID, "inbox_id", inboxID)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no session linked to this inbox", "inbox_id": strconv.Itoa(inboxID)})
		return
	}
	sender := asMap(asMap(res["meta"])["sender"])
	name := asStr(sender["name"])
	contactID := asInt(sender["id"])
	sourceID := asStr(asMap(res["contact_inbox"])["source_id"])
	phone := ""
	if ca := asMap(sender["custom_attributes"]); ca != nil {
		raw := asStr(ca[cwChatIDAttr])
		if raw != "" {
			if jid, e := types.ParseJID(raw); e == nil {
				phone = sess.realPhone(jid) // converte LID->PN se necessário
			} else {
				phone = digitsOnly(raw)
			}
		}
	}
	if phone == "" {
		phone = digitsOnly(asStr(sender["phone_number"]))
	}
	if phone == "" {
		s.log.Warn("chatwoot resolve: contact has no phone", "conversation_id", convID)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "contact has no phone"})
		return
	}
	if sourceID == "" {
		sourceID = buildChatwootSourceID(sess.id, phone, chatwootRuntime().SourceStrategy)
	}
	s.log.Info("chatwoot resolve ok", "session", sess.id, "inbox_id", inboxID, "phone", phone, "name", name)
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id": sess.id, "account_id": accountID, "inbox_id": inboxID,
		"conversation_id": asInt(convID), "contact_id": contactID,
		"source_id": sourceID, "phone": phone, "name": name,
	})
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ---------- handlers de config ----------

func (s *server) handleSetChatwoot(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var cfg ChatwootConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	// se o token vier vazio (edição), mantém o atual
	if cfg.AccountToken == "" {
		cfg.AccountToken = sess.getChatwoot().AccountToken
	}
	if !cfg.valid() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "url, account_id, account_token e inbox_id são obrigatórios"})
		return
	}
	sess.setChatwoot(cfg)
	b, _ := json.Marshal(cfg)
	_ = sess.mgr.store.setChatwoot(r.Context(), sess.id, string(b))
	writeJSON(w, http.StatusOK, map[string]any{"chatwoot": cfg})
}

func (s *server) handleGetChatwoot(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	cfg := sess.getChatwoot()
	cfg.AccountToken = "" // não devolve o token
	writeJSON(w, http.StatusOK, map[string]any{"chatwoot": cfg, "enabled": sess.getChatwoot().valid()})
}

func (s *server) handleDeleteChatwoot(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	sess.setChatwoot(ChatwootConfig{})
	_ = sess.mgr.store.setChatwoot(r.Context(), sess.id, "")
	w.WriteHeader(http.StatusNoContent)
}

// ---------- helpers de JSON dinâmico ----------

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
func asList(v any) []any         { l, _ := v.([]any); return l }
func asStr(v any) string         { s, _ := v.(string); return s }
func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func firstNonZero(a, b int) int {
	if a != 0 {
		return a
	}
	return b
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func conversationFromMap(m map[string]any) chatwootConversation {
	payload := asMap(m["payload"])
	if len(payload) > 0 {
		m = payload
	}
	conv := chatwootConversation{
		ID:        asInt(m["id"]),
		InboxID:   firstNonZero(asInt(m["inbox_id"]), asInt(asMap(m["inbox"])["id"])),
		ContactID: firstNonZero(asInt(m["contact_id"]), asInt(asMap(asMap(m["meta"])["sender"])["id"])),
		SourceID:  firstNonEmpty(asStr(m["source_id"]), asStr(asMap(m["contact_inbox"])["source_id"])),
		Status:    asStr(m["status"]),
		UpdatedAt: firstNonZero64(asInt64(m["last_activity_at"]), asInt64(m["updated_at"])),
	}
	if conv.UpdatedAt == 0 {
		conv.UpdatedAt = int64(conv.ID)
	}
	return conv
}

func firstNonZero64(a, b int64) int64 {
	if a != 0 {
		return a
	}
	return b
}

func withinChatwootReopenWindow(conv chatwootConversation, days int) bool {
	if days <= 0 || conv.UpdatedAt == 0 {
		return true
	}
	last := conv.UpdatedAt
	if last < 1_000_000_000_000 {
		last *= 1000
	}
	return time.Since(time.UnixMilli(last)) <= time.Duration(days)*24*time.Hour
}

// downloadableOf devolve a parte de mídia da mensagem (ou nil se for texto).
func downloadableOf(m *waE2E.Message) whatsmeow.DownloadableMessage {
	switch {
	case m.GetImageMessage() != nil:
		return m.GetImageMessage()
	case m.GetAudioMessage() != nil:
		return m.GetAudioMessage()
	case m.GetVideoMessage() != nil:
		return m.GetVideoMessage()
	case m.GetDocumentMessage() != nil:
		return m.GetDocumentMessage()
	}
	return nil
}

// mediaMeta devolve (filename, mimetype) p/ a mídia recebida.
func mediaMeta(m *waE2E.Message) (string, string) {
	switch {
	case m.GetImageMessage() != nil:
		return "image.jpg", firstNonEmpty(m.GetImageMessage().GetMimetype(), "image/jpeg")
	case m.GetAudioMessage() != nil:
		return "audio.ogg", firstNonEmpty(m.GetAudioMessage().GetMimetype(), "audio/ogg")
	case m.GetVideoMessage() != nil:
		return "video.mp4", firstNonEmpty(m.GetVideoMessage().GetMimetype(), "video/mp4")
	case m.GetDocumentMessage() != nil:
		d := m.GetDocumentMessage()
		return firstNonEmpty(d.GetFileName(), "file"), firstNonEmpty(d.GetMimetype(), "application/octet-stream")
	}
	return "file", "application/octet-stream"
}

func sourceIDForInbox(contact map[string]any, inboxID int) string {
	for _, ci := range asList(contact["contact_inboxes"]) {
		m := asMap(ci)
		if firstNonZero(asInt(asMap(m["inbox"])["id"]), asInt(m["inbox_id"])) == inboxID {
			return asStr(m["source_id"])
		}
	}
	return ""
}
