package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecordingPolicy(t *testing.T) {
	if !((RecordingConfig{Enabled: true, Mode: "always"}).shouldRecord(false)) {
		t.Fatal("always mode must record even when request is false")
	}
	if (RecordingConfig{Enabled: true, Mode: "off"}).shouldRecord(true) {
		t.Fatal("off mode must not record")
	}
	if (RecordingConfig{Enabled: true, Mode: "on_demand"}).shouldRecord(false) {
		t.Fatal("on_demand must respect false request")
	}
	if !((RecordingConfig{Enabled: true, Mode: "on_demand"}).shouldRecord(true)) {
		t.Fatal("on_demand must respect true request")
	}
}

func TestCallRecorderWritesWAV(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewCallRecorder(RecordingConfig{
		Enabled: true, Mode: "always", Dir: dir, Format: "wav", MaxFileMB: 100,
	}, RecordingMeta{SessionID: "s1", CallID: "call-1"})
	if err != nil {
		t.Fatal(err)
	}
	rec.WriteOperatorPCM16([]float32{0, 0.5, -0.5, 1})
	rec.WritePeerPCM16([]float32{0.25, -0.25})
	time.Sleep(20 * time.Millisecond)
	result, err := rec.Stop()
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.SizeBytes <= 44 {
		t.Fatalf("unexpected result: %+v", result)
	}
	data, err := os.ReadFile(filepath.Join(dir, result.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" || string(data[36:40]) != "data" {
		t.Fatalf("invalid wav header: %q %q %q", data[0:4], data[8:12], data[36:40])
	}
}
