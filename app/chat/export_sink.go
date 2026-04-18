package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-faster/jx"
	"go.uber.org/multierr"

	"github.com/iyear/tdl/pkg/supabase"
)

type exportSink interface {
	Add(msg *Message) error
	Close() error
}

func newExportSink(ctx context.Context, opts ExportOptions, chatID int64) (exportSink, error) {
	if opts.Supabase {
		return newSupabaseExportSink(ctx, opts.SupabaseConfig, chatID, opts.Thread)
	}

	return newFileExportSink(opts.Output, chatID)
}

type fileExportSink struct {
	file *os.File
	enc  *jx.Encoder
}

func newFileExportSink(path string, chatID int64) (*fileExportSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	enc := jx.NewStreamingEncoder(f, 512)

	enc.ObjStart()
	enc.Field("id", func(e *jx.Encoder) { e.Int64(chatID) })
	enc.FieldStart("messages")
	enc.ArrStart()

	return &fileExportSink{
		file: f,
		enc:  enc,
	}, nil
}

func (f *fileExportSink) Add(msg *Message) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	f.enc.Raw(raw)
	return nil
}

func (f *fileExportSink) Close() error {
	if f.enc != nil {
		f.enc.ArrEnd()
		f.enc.ObjEnd()
	}

	return multierr.Combine(
		f.enc.Close(),
		f.file.Close(),
	)
}

type supabaseExportSink struct {
	ctx    context.Context
	client *supabase.Client

	chatID  int64
	topicID int

	batch []supabase.MessageRow
	seq   int64
}

func newSupabaseExportSink(
	ctx context.Context,
	configPath string,
	chatID int64,
	topicID int,
) (*supabaseExportSink, error) {
	client, err := supabase.Load(configPath)
	if err != nil {
		return nil, err
	}

	if err = client.DeleteMessages(ctx, chatID, topicID); err != nil {
		return nil, fmt.Errorf("clear existing supabase rows: %w", err)
	}

	return &supabaseExportSink{
		ctx:     ctx,
		client:  client,
		chatID:  chatID,
		topicID: topicID,
		batch:   make([]supabase.MessageRow, 0, client.BatchSize()),
		seq:     0,
	}, nil
}

func (s *supabaseExportSink) Add(msg *Message) error {
	s.seq++
	s.batch = append(s.batch, supabase.MessageRow{
		ChatID:    s.chatID,
		TopicID:   s.topicID,
		MessageID: msg.ID,
		ExportSeq: s.seq,
	})

	if len(s.batch) >= s.client.BatchSize() {
		return s.flush()
	}

	return nil
}

func (s *supabaseExportSink) flush() error {
	if len(s.batch) == 0 {
		return nil
	}

	if err := s.client.InsertMessages(s.ctx, s.batch); err != nil {
		return fmt.Errorf("insert supabase rows: %w", err)
	}

	s.batch = s.batch[:0]
	return nil
}

func (s *supabaseExportSink) Close() error {
	return s.flush()
}
