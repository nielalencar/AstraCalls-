package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	recordingStatusRecording    = "recording"
	recordingStatusFinalizing   = "finalizing"
	recordingStatusSaved        = "saved"
	recordingStatusUploading    = "uploading"
	recordingStatusAttached     = "attached"
	recordingStatusUploadFailed = "upload_failed"
	recordingStatusFailed       = "failed"
)

type RecordingConfig struct {
	Enabled       bool
	Mode          string
	Dir           string
	Format        string
	PrivateNote   bool
	RetentionDays int
	UploadRetry   bool
	MaxFileMB      int
}

type RecordingMeta struct {
	SessionID              string
	CallID                 string
	Direction              string
	Peer                   string
	Phone                  string
	SourceID               string
	Owner                  string
	ChatwootAccountID      int
	ChatwootInboxID        int
	ChatwootContactID      int
	ChatwootConversationID int
}

type CallChatwootContext struct {
	AccountID      int    `json:"account_id"`
	InboxID        int    `json:"inbox_id"`
	ContactID      int    `json:"contact_id"`
	ConversationID int    `json:"conversation_id"`
	SourceID       string `json:"source_id"`
}

type RecordingResult struct {
	FilePath   string
	FileName   string
	MimeType   string
	SizeBytes  int64
	DurationMs int64
	StartedAt  int64
	EndedAt    int64
	Drops      int64
}

type recordingFrame struct {
	samples []float32
}

type CallRecorder struct {
	mu        sync.Mutex
	cfg       RecordingConfig
	meta      RecordingMeta
	file      *os.File
	filePath  string
	fileName  string
	startedAt time.Time
	closed    bool
	frames    chan recordingFrame
	done      chan struct{}
	samples   int64
	drops     int64
	bytes     int64
	writeErr  error
}

func recordingConfigFromEnv() RecordingConfig {
	return RecordingConfig{
		Enabled:       envBool("WACALLS_RECORDINGS_ENABLED", true),
		Mode:          envStr("WACALLS_RECORDINGS_MODE", "always"),
		Dir:           envStr("WACALLS_RECORDINGS_DIR", "/app/recordings"),
		Format:        envStr("WACALLS_RECORDINGS_FORMAT", "wav"),
		PrivateNote:   envBool("WACALLS_RECORDINGS_PRIVATE_NOTE", true),
		RetentionDays: envInt("WACALLS_RECORDINGS_RETENTION_DAYS", 90),
		UploadRetry:   envBool("WACALLS_RECORDINGS_UPLOAD_RETRY", true),
		MaxFileMB:      envInt("WACALLS_RECORDINGS_MAX_FILE_MB", 100),
	}
}

func (c RecordingConfig) shouldRecord(requested bool) bool {
	if !c.Enabled {
		return false
	}
	switch strings.ToLower(c.Mode) {
	case "off":
		return false
	case "on_demand":
		return requested
	default:
		return true
	}
}

func NewCallRecorder(cfg RecordingConfig, meta RecordingMeta) (*CallRecorder, error) {
	if strings.ToLower(cfg.Format) != "wav" {
		return nil, fmt.Errorf("unsupported recording format %q", cfg.Format)
	}
	if cfg.Dir == "" {
		cfg.Dir = "/app/recordings"
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("call_%s.wav", safeFilePart(meta.CallID))
	path := filepath.Join(cfg.Dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	r := &CallRecorder{
		cfg:       cfg,
		meta:      meta,
		file:      f,
		filePath:  path,
		fileName:  name,
		startedAt: time.Now(),
		frames:    make(chan recordingFrame, 128),
		done:      make(chan struct{}),
	}
	if err := writeWAVHeader(f, 16000, 1, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	go r.loop()
	return r, nil
}

func (r *CallRecorder) WriteOperatorPCM16(samples []float32) { r.enqueue(samples) }
func (r *CallRecorder) WritePeerPCM16(samples []float32)     { r.enqueue(samples) }

func (r *CallRecorder) enqueue(samples []float32) {
	if r == nil || len(samples) == 0 {
		return
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return
	}
	cp := append([]float32(nil), samples...)
	select {
	case r.frames <- recordingFrame{samples: cp}:
	default:
		r.mu.Lock()
		r.drops++
		r.mu.Unlock()
	}
}

func (r *CallRecorder) loop() {
	defer close(r.done)
	for fr := range r.frames {
		if err := r.writeSamples(fr.samples); err != nil {
			r.mu.Lock()
			if r.writeErr == nil {
				r.writeErr = err
			}
			r.mu.Unlock()
		}
	}
}

func (r *CallRecorder) writeSamples(samples []float32) error {
	if r.cfg.MaxFileMB > 0 && r.bytes >= int64(r.cfg.MaxFileMB)*1024*1024 {
		r.mu.Lock()
		r.drops++
		r.mu.Unlock()
		return nil
	}
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		if s > 1 {
			s = 1
		} else if s < -1 {
			s = -1
		}
		v := int16(math.Round(float64(s) * 32767))
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(v))
	}
	n, err := r.file.Write(buf)
	r.mu.Lock()
	r.samples += int64(len(samples))
	r.bytes += int64(n)
	r.mu.Unlock()
	return err
}

func (r *CallRecorder) Stop() (*RecordingResult, error) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, nil
	}
	r.closed = true
	r.mu.Unlock()
	close(r.frames)
	<-r.done

	r.mu.Lock()
	dataBytes := r.bytes
	samples := r.samples
	drops := r.drops
	writeErr := r.writeErr
	r.mu.Unlock()

	if _, err := r.file.Seek(0, 0); err != nil && writeErr == nil {
		writeErr = err
	}
	if err := writeWAVHeader(r.file, 16000, 1, uint32(dataBytes)); err != nil && writeErr == nil {
		writeErr = err
	}
	if err := r.file.Close(); err != nil && writeErr == nil {
		writeErr = err
	}
	info, statErr := os.Stat(r.filePath)
	if statErr != nil && writeErr == nil {
		writeErr = statErr
	}
	ended := time.Now()
	size := int64(44) + dataBytes
	if info != nil {
		size = info.Size()
	}
	return &RecordingResult{
		FilePath:   r.filePath,
		FileName:   r.fileName,
		MimeType:   "audio/wav",
		SizeBytes:  size,
		DurationMs: samples * 1000 / 16000,
		StartedAt:  r.startedAt.UnixMilli(),
		EndedAt:    ended.UnixMilli(),
		Drops:      drops,
	}, writeErr
}

func (r *CallRecorder) Abort(reason error) {
	if r == nil {
		return
	}
	_, _ = r.Stop()
}

func writeWAVHeader(f *os.File, sampleRate, channels int, dataBytes uint32) error {
	byteRate := uint32(sampleRate * channels * 2)
	blockAlign := uint16(channels * 2)
	header := make([]byte, 44)
	copy(header[0:], "RIFF")
	binary.LittleEndian.PutUint32(header[4:], 36+dataBytes)
	copy(header[8:], "WAVE")
	copy(header[12:], "fmt ")
	binary.LittleEndian.PutUint32(header[16:], 16)
	binary.LittleEndian.PutUint16(header[20:], 1)
	binary.LittleEndian.PutUint16(header[22:], uint16(channels))
	binary.LittleEndian.PutUint32(header[24:], uint32(sampleRate))
	binary.LittleEndian.PutUint32(header[28:], byteRate)
	binary.LittleEndian.PutUint16(header[32:], blockAlign)
	binary.LittleEndian.PutUint16(header[34:], 16)
	copy(header[36:], "data")
	binary.LittleEndian.PutUint32(header[40:], dataBytes)
	_, err := f.Write(header)
	return err
}

func safeFilePart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return newStoreID()
	}
	return b.String()
}
