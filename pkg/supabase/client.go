package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultConfigPath = "tdl-supabase.json"
	DefaultSchema     = "public"
	DefaultTable      = "tdl_messages"
	DefaultBatchSize  = 500

	pageSize = 1000
)

type Config struct {
	URL            string `json:"url"`
	AnonKey        string `json:"anon_key"`
	ServiceRoleKey string `json:"service_role_key"`
	Schema         string `json:"schema"`
	Table          string `json:"table"`
	BatchSize      int    `json:"batch_size"`
}

func LoadConfig(path string) (Config, error) {
	if path == "" {
		path = DefaultConfigPath
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read supabase config %q: %w", path, err)
	}

	cfg := Config{}
	if err = json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode supabase config %q: %w", path, err)
	}

	if err = cfg.normalize(); err != nil {
		return Config{}, fmt.Errorf("invalid supabase config %q: %w", path, err)
	}

	return cfg, nil
}

func (c *Config) normalize() error {
	c.URL = strings.TrimSpace(strings.TrimRight(c.URL, "/"))
	c.AnonKey = strings.TrimSpace(c.AnonKey)
	c.ServiceRoleKey = strings.TrimSpace(c.ServiceRoleKey)
	c.Schema = strings.TrimSpace(c.Schema)
	c.Table = strings.TrimSpace(c.Table)

	switch {
	case c.URL == "":
		return fmt.Errorf("url is required")
	case c.ServiceRoleKey == "" && c.AnonKey == "":
		return fmt.Errorf("either service_role_key or anon_key is required")
	}

	if c.Schema == "" {
		c.Schema = DefaultSchema
	}

	if c.Table == "" {
		c.Table = DefaultTable
	}

	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}

	return nil
}

func (c Config) token() string {
	if c.ServiceRoleKey != "" {
		return c.ServiceRoleKey
	}

	return c.AnonKey
}

type Client struct {
	cfg Config

	endpoint string
	token    string
	client   *http.Client
}

type MessageRow struct {
	ChatID    int64 `json:"chat_id"`
	TopicID   int   `json:"topic_id"`
	MessageID int   `json:"message_id"`
	ExportSeq int64 `json:"export_seq"`
}

func New(cfg Config) (*Client, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	return &Client{
		cfg:      cfg,
		endpoint: fmt.Sprintf("%s/rest/v1/%s", cfg.URL, url.PathEscape(cfg.Table)),
		token:    cfg.token(),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func Load(path string) (*Client, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}

	return New(cfg)
}

func (c *Client) BatchSize() int {
	return c.cfg.BatchSize
}

func (c *Client) DeleteMessages(ctx context.Context, chatID int64, topicID int) error {
	query := url.Values{}
	query.Set("chat_id", fmt.Sprintf("eq.%d", chatID))
	query.Set("topic_id", fmt.Sprintf("eq.%d", topicID))

	req, err := c.newRequest(ctx, http.MethodDelete, query, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Prefer", "return=minimal")
	return c.do(req, nil)
}

func (c *Client) DeleteMessage(ctx context.Context, chatID int64, topicID int, messageID int) error {
	query := url.Values{}
	query.Set("chat_id", fmt.Sprintf("eq.%d", chatID))
	query.Set("topic_id", fmt.Sprintf("eq.%d", topicID))
	query.Set("message_id", fmt.Sprintf("eq.%d", messageID))

	req, err := c.newRequest(ctx, http.MethodDelete, query, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Prefer", "return=minimal")
	return c.do(req, nil)
}

func (c *Client) InsertMessages(ctx context.Context, rows []MessageRow) error {
	if len(rows) == 0 {
		return nil
	}

	query := url.Values{}
	query.Set("on_conflict", "chat_id,topic_id,message_id")

	req, err := c.newRequest(ctx, http.MethodPost, query, rows)
	if err != nil {
		return err
	}

	req.Header.Set("Prefer", "resolution=merge-duplicates,return=minimal")
	return c.do(req, nil)
}

func (c *Client) FetchMessageIDs(ctx context.Context, chatID int64, topicID int) ([]int, error) {
	type row struct {
		MessageID int `json:"message_id"`
	}

	ids := make([]int, 0)
	offset := 0

	for {
		query := url.Values{}
		query.Set("chat_id", fmt.Sprintf("eq.%d", chatID))
		query.Set("topic_id", fmt.Sprintf("eq.%d", topicID))
		query.Set("select", "message_id")
		query.Set("order", "export_seq.asc,message_id.asc")
		query.Set("limit", strconv.Itoa(pageSize))
		query.Set("offset", strconv.Itoa(offset))

		req, err := c.newRequest(ctx, http.MethodGet, query, nil)
		if err != nil {
			return nil, err
		}

		rows := make([]row, 0)
		if err = c.do(req, &rows); err != nil {
			return nil, err
		}

		for _, r := range rows {
			ids = append(ids, r.MessageID)
		}

		if len(rows) < pageSize {
			break
		}

		offset += pageSize
	}

	return ids, nil
}

func (c *Client) newRequest(
	ctx context.Context,
	method string,
	query url.Values,
	body interface{},
) (*http.Request, error) {
	endpoint := c.endpoint
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		payload = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, payload)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("apikey", c.token)
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.cfg.Schema != "" && c.cfg.Schema != DefaultSchema {
		req.Header.Set("Accept-Profile", c.cfg.Schema)
		if method != http.MethodGet && method != http.MethodHead {
			req.Header.Set("Content-Profile", c.cfg.Schema)
		}
	}

	return req, nil
}

func (c *Client) do(req *http.Request, out interface{}) error {
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = "<empty>"
		}
		return fmt.Errorf("supabase request failed [%s %s]: status=%s body=%s",
			req.Method, req.URL.Redacted(), resp.Status, message)
	}

	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	if err = json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
