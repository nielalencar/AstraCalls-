package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/wanode"
	"wacalls/internal/wa"

	"database/sql"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Session struct {
	id   string
	name string
	mgr  *SessionManager
	log  *slog.Logger

	client *whatsmeow.Client
	reg    *callRegistry

	// store próprio desta sessão (1 banco por sessão, estilo WAHA)
	waContainer *sqlstore.Container
	waDB        *sql.DB

	mu       sync.Mutex
	auth     AuthSnapshot
	webhook  string
	chatwoot ChatwootConfig
}

func (s *Session) setWebhook(url string) {
	s.mu.Lock()
	s.webhook = url
	s.mu.Unlock()
}

func (s *Session) getWebhook() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.webhook
}

func (s *Session) setChatwoot(c ChatwootConfig) {
	s.mu.Lock()
	s.chatwoot = c
	s.mu.Unlock()
}

func (s *Session) getChatwoot() ChatwootConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chatwoot
}

func newSession(mgr *SessionManager, id, name string, client *whatsmeow.Client) *Session {
	s := &Session{
		id:     id,
		name:   name,
		mgr:    mgr,
		log:    mgr.log.With("session", id),
		client: client,
		auth:   AuthSnapshot{State: "connecting"},
		reg:    newCallRegistry(),
	}
	client.AddEventHandler(s.handleEvent)
	return s
}

func (s *Session) createCall(callID string) *call.CallManager {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log)
	s.wireCall(cm, callID)
	s.reg.add(callID, &activeCall{cm: cm})
	return cm
}

func (s *Session) wireCall(cm *call.CallManager, callID string) {
	cm.OnIncoming = func(c *call.CallInfo) {
		s.mgr.broker.upsertCall(CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: "inbound", Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
		})
		s.mgr.broker.emitIncoming(s.id, c.CallID, c.PeerJid)
	}
	cm.OnStateChange = func(c *call.CallInfo) {
		if c.IsEnded() {
			s.removeCall(c.CallID)
			s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
			return
		}
		dir := "outbound"
		if c.Direction == core.CallDirectionIncoming {
			dir = "inbound"
		}
		existing, _ := s.mgr.broker.getCall(c.CallID)
		rec := CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: dir, Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: mapStatus(c.StateData.State),
		}
		if existing != nil {
			rec.Owner = existing.Owner
			rec.StartedAt = existing.StartedAt
		}
		s.mgr.broker.upsertCall(rec)
	}
	cm.OnEnded = func(c *call.CallInfo) {
		s.removeCall(c.CallID)
		s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
	}
	cm.OnPeerAudio = func(pcm16 []float32) {
		ac, ok := s.reg.get(callID)
		if !ok {
			return
		}
		if ac.recorder != nil {
			ac.recorder.WritePeerPCM16(pcm16)
		}
		if ac.bridge == nil || ac.browserOpus == nil {
			return
		}
		pcm48 := media.Upsample16to48(pcm16)
		opus, err := ac.browserOpus.Encode(pcm48)
		if err != nil || len(opus) == 0 {
			return
		}
		_ = ac.bridge.WriteOpus(opus, 60*time.Millisecond)
	}
}

func (s *Session) startRecordingForCall(callID, direction, peer, owner string, requested bool, cw CallChatwootContext) {
	cfg := recordingConfigFromEnv()
	if !cfg.shouldRecord(requested) {
		return
	}
	ac, ok := s.reg.get(callID)
	if !ok || ac.recorder != nil {
		return
	}
	phone := normalizeChatwootPhone(peer)
	if jid, err := types.ParseJID(peer); err == nil {
		phone = normalizeChatwootPhone(jid.User)
	}
	sourceID := cw.SourceID
	if sourceID == "" {
		sourceID = buildChatwootSourceID(s.id, phone, chatwootRuntime().SourceStrategy)
	}
	chatCfg := s.getChatwoot()
	accountID, inboxID := cw.AccountID, cw.InboxID
	if accountID == 0 {
		accountID = chatCfg.AccountID
	}
	if inboxID == 0 {
		inboxID = chatCfg.InboxID
	}
	meta := RecordingMeta{
		SessionID:              s.id,
		CallID:                 callID,
		Direction:              direction,
		Peer:                   peer,
		Phone:                  phone,
		SourceID:               sourceID,
		Owner:                  owner,
		ChatwootAccountID:      accountID,
		ChatwootInboxID:        inboxID,
		ChatwootContactID:      cw.ContactID,
		ChatwootConversationID: cw.ConversationID,
	}
	recorder, err := NewCallRecorder(cfg, meta)
	if err != nil {
		s.log.Error("recording_started failed", "call_id", callID, "err", err)
		_ = s.mgr.store.createCallRecording(s.mgr.appCtx, callRecordingRow{
			ID: newStoreID(), SessionID: s.id, CallID: callID, Direction: direction, Peer: peer, Phone: phone,
			SourceID: sourceID, Owner: owner, ChatwootAccountID: accountID, ChatwootInboxID: inboxID,
			ChatwootContactID: cw.ContactID, ChatwootConversationID: cw.ConversationID,
			Status: recordingStatusFailed, Error: err.Error(), StartedAt: time.Now().UnixMilli(),
		})
		return
	}
	recID := newStoreID()
	ac.recorder = recorder
	ac.recordingID = recID
	ac.recording = meta
	if err := s.mgr.store.createCallRecording(s.mgr.appCtx, callRecordingRow{
		ID: recID, SessionID: s.id, CallID: callID, Direction: direction, Peer: peer, Phone: phone,
		SourceID: sourceID, Owner: owner, ChatwootAccountID: accountID, ChatwootInboxID: inboxID,
		ChatwootContactID: cw.ContactID, ChatwootConversationID: cw.ConversationID,
		Status: recordingStatusRecording, FilePath: recorder.filePath, FileName: recorder.fileName,
		MimeType: "audio/wav", StartedAt: recorder.startedAt.UnixMilli(),
	}); err != nil {
		s.log.Error("recording_started persistence failed", "call_id", callID, "err", err)
	}
	s.log.Info("recording_started", "call_id", callID, "file", recorder.filePath)
}

func (s *Session) startOutgoing(ctx context.Context, peer types.JID, isVideo bool) (string, error) {
	callID := signaling.GenerateCallID()
	cm := s.createCall(callID)
	if err := cm.StartCall(ctx, callID, peer, isVideo); err != nil {
		s.removeCall(callID)
		return "", err
	}
	return callID, nil
}

func (s *Session) callForEvent(from types.JID, data *waBinary.Node) (*activeCall, bool) {
	callID := callIDFromNode(wrapCall(from, data))
	if callID == "" {
		return nil, false
	}
	return s.reg.get(callID)
}

func (s *Session) onIncomingOffer(ctx context.Context, evt *events.CallOffer) {
	node := wrapCall(evt.From, evt.Data)
	callID := callIDFromNode(node)
	if callID == "" {
		return
	}
	if max := s.mgr.maxCalls; max > 0 && s.reg.count() >= max {
		s.rejectOffer(ctx, node, evt.From)
		return
	}
	cm := s.createCall(callID)
	cm.HandleCallOffer(ctx, node, evt.From)
}

func (s *Session) rejectOffer(ctx context.Context, node *waBinary.Node, from types.JID) {
	info := signaling.ExtractNodeInfo(node)
	if info == nil {
		return
	}
	creator := wanode.AttrString(info.InnerNode.Attrs, "call-creator")
	if creator == "" {
		creator = from.String()
	}
	reject := signaling.BuildRejectStanza(from, info.CallID, wanode.MustJID(creator))
	_ = wa.NewSocket(s.client).SendNode(ctx, reject)
	s.log.Info("inbound call rejected: session at capacity", "call_id", info.CallID)
}

func (s *Session) handleEvent(rawEvt any) {
	ctx := context.Background()
	switch evt := rawEvt.(type) {
	case *events.Connected:
		if id := s.client.Store.ID; id != nil {
			_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
		}
		s.setAuth(AuthSnapshot{State: "open", Paired: true})
	case *events.LoggedOut:
		s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
	case *events.Message:
		s.dispatchWebhook("message", summarizeMessage(evt))
		go s.chatwootPushIncoming(evt)
	case *events.Receipt:
		s.dispatchWebhook("receipt", map[string]any{
			"chat": evt.Chat.String(), "sender": evt.Sender.String(),
			"type": string(evt.Type), "ids": evt.MessageIDs,
			"timestamp": evt.Timestamp.UnixMilli(),
		})
	case *events.CallOffer:
		s.onIncomingOffer(ctx, evt)
	case *events.CallAccept:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallAccept(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTransport:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTransport(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTerminate:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	case *events.CallReject:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	}
}

func (s *Session) connect(ctx context.Context) error {
	if s.client.Store.ID != nil {
		return s.client.Connect()
	}
	return s.startPairing(ctx)
}

func (s *Session) startPairing(ctx context.Context) error {
	qrChan, err := s.client.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := s.client.Connect(); err != nil {
		return err
	}
	go func() {
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				s.log.Info("scan the QR code to pair this session")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				s.setAuth(AuthSnapshot{State: "qr", QR: evt.Code})
				s.mgr.broker.emitSessionQR(s.id, evt.Code)
			case "success":
				if id := s.client.Store.ID; id != nil {
					_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
				}
				s.setAuth(AuthSnapshot{State: "open", Paired: true})
			case "timeout":
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			}
		}
	}()
	return nil
}

func (s *Session) setAuth(a AuthSnapshot) {
	s.mu.Lock()
	s.auth = a
	s.mu.Unlock()
	s.mgr.broker.emitAuthState(s.id, a)
	s.mgr.broker.emitSessionList(s.mgr.infos())
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	a := s.auth
	s.mu.Unlock()
	jid := ""
	if id := s.client.Store.ID; id != nil {
		jid = id.String()
	}
	return SessionInfo{ID: s.id, Name: s.name, JID: jid, State: a.State, Paired: a.Paired || jid != ""}
}

func (s *Session) setBridge(callID string, b *Bridge, oc media.Codec) {
	oldB, oldOC, found := s.reg.setBridge(callID, b, oc)
	if !found {
		b.Close()
		if oc != nil {
			oc.Close()
		}
		return
	}
	if oldB != nil {
		oldB.Close()
	}
	if oldOC != nil {
		oldOC.Close()
	}
}

func (s *Session) removeCall(callID string) {
	ac, ok := s.reg.remove(callID)
	if !ok {
		return
	}
	s.finalizeRecording(callID, ac)
	if ac.bridge != nil {
		ac.bridge.Close()
	}
	if ac.browserOpus != nil {
		ac.browserOpus.Close()
	}
}

func (s *Session) terminateCall(callID string, reason core.EndCallReason) {
	ac, ok := s.reg.get(callID)
	if !ok {
		return
	}
	_ = ac.cm.EndCall(context.Background(), reason)
}

func (s *Session) teardownAllCalls() {
	for _, ac := range s.reg.drain() {
		_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		s.finalizeRecording("", ac)
		if ac.bridge != nil {
			ac.bridge.Close()
		}
		if ac.browserOpus != nil {
			ac.browserOpus.Close()
		}
	}
}

func (s *Session) finalizeRecording(callID string, ac *activeCall) {
	if ac == nil || ac.recorder == nil || ac.recordingID == "" {
		return
	}
	recorder := ac.recorder
	ac.recorder = nil
	_ = s.mgr.store.updateCallRecordingFailed(s.mgr.appCtx, ac.recordingID, recordingStatusFinalizing, "")
	result, err := recorder.Stop()
	if err != nil {
		msg := err.Error()
		_ = s.mgr.store.updateCallRecordingFailed(s.mgr.appCtx, ac.recordingID, recordingStatusFailed, msg)
		s.log.Error("recording_finalized failed", "call_id", firstNonEmpty(callID, ac.recording.CallID), "err", err)
		return
	}
	if result == nil {
		return
	}
	if err := s.mgr.store.updateCallRecordingSaved(s.mgr.appCtx, ac.recordingID, *result); err != nil {
		s.log.Error("recording_finalized persistence failed", "call_id", firstNonEmpty(callID, ac.recording.CallID), "err", err)
	}
	s.log.Info("recording_finalized", "call_id", firstNonEmpty(callID, ac.recording.CallID), "file", result.FilePath, "duration_ms", result.DurationMs, "size_bytes", result.SizeBytes, "drops", result.Drops)
	go s.mgr.uploadCallRecordingByID(ac.recordingID)
}

func (s *Session) replaceClient(client *whatsmeow.Client) {
	s.teardownAllCalls()
	s.client.Disconnect()
	s.client = client
	client.AddEventHandler(s.handleEvent)
}

func (s *Session) shutdown() {
	s.teardownAllCalls()
	s.client.Disconnect()
	if s.waDB != nil {
		_ = s.waDB.Close()
	}
}

func mapStatus(state core.CallState) CallStatus {
	switch state {
	case core.CallStateActive:
		return StatusConnected
	case core.CallStateEnded:
		return StatusEnded
	case core.CallStateInitiating:
		return StatusStarting
	default:
		return StatusRinging
	}
}
