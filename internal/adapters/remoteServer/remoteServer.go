package remoteServer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type RemoteServer struct {
	Url string
}

func New(url string) *RemoteServer {
	return &RemoteServer{url}
}

func (rs *RemoteServer) Send(ctx context.Context, topic string, headers map[string]string, key, value []byte, timestamp time.Time, offset int64) ([]byte, error) {
	data := struct {
		Topic     string            `json:"topic"`
		Headers   map[string]string `json:"headers"`
		Key       string            `json:"key"`
		Value     json.RawMessage   `json:"value"`
		Timestamp int64             `json:"timestamp"`
		Offset    int64             `json:"offset"`
	}{
		Topic:     topic,
		Headers:   headers,
		Key:       string(key),
		Value:     value,
		Timestamp: timestamp.UnixMilli(),
		Offset:    offset,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal payload error: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		rs.Url,
		bytes.NewBuffer(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("make request error: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request error: %w", err)
	}
	defer func() {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid response code %d, %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response error: %w", err)
	}

	return body, nil
}
