package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string, timeout time.Duration) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: timeout}).DialContext
	transport.TLSHandshakeTimeout = timeout
	// NOTE: Do not set ResponseHeaderTimeout here.
	// Long-running SQL requests may legitimately take a long time before the server sends headers.
	// We only want client-side timeout to apply to connecting to the backend (dial/TLS).

	return &Client{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Transport: transport,
		},
	}
}

type ConnectRequest struct {
	Dsn     string         `json:"dsn"`
	DbType  string         `json:"db_type"`
	Options ConnectOptions `json:"options"`
}

type ConnectOptions struct {
	ReadOnly            bool `json:"read_only"`
	UseMcp              bool `json:"use_mcp"`
	ConnectionTimeoutMs int  `json:"connection_timeout_ms"`
}

type ConnectResponse struct {
	SessionId string    `json:"session_id"`
	TraceId   string    `json:"trace_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type ExecuteRequest struct {
	SessionId string         `json:"session_id"`
	Sql       string         `json:"sql"`
	Options   ExecuteOptions `json:"options"`
}

type ExecuteOptions struct {
	Limit          int `json:"limit"`
	FetchSize      int `json:"fetch_size"`
	QueryTimeoutMs int `json:"query_timeout_ms"`
}

type ExecuteResponse struct {
	Type     string           `json:"type"`
	Data     DataContent      `json:"data"`
	Metadata ResponseMetadata `json:"metadata"`
}

type DataContent struct {
	TextContent string                   `json:"text_content,omitempty"`
	Columns     []ColumnDefinition       `json:"columns,omitempty"`
	Rows        []map[string]interface{} `json:"rows,omitempty"`
}

type ColumnDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type ResponseMetadata struct {
	DurationMs   int  `json:"duration_ms"`
	RowsAffected int  `json:"rows_affected"`
	Truncated    bool `json:"truncated"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	TraceId string `json:"trace_id"`
}

func (c *Client) Connect(req *ConnectRequest) (*ConnectResponse, error) {
	url := fmt.Sprintf("%s/v1/connect", c.BaseURL)
	respBody, err := c.post(url, req)
	if err != nil {
		return nil, err
	}
	defer respBody.Close()

	var resp ConnectResponse
	if err := json.NewDecoder(respBody).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Execute(req *ExecuteRequest) (*ExecuteResponse, error) {
	url := fmt.Sprintf("%s/v1/execute", c.BaseURL)
	respBody, err := c.post(url, req)
	if err != nil {
		return nil, err
	}
	defer respBody.Close()

	var resp ExecuteResponse
	if err := json.NewDecoder(respBody).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Disconnect(sessionId string) error {
	url := fmt.Sprintf("%s/v1/disconnect?session_id=%s", c.BaseURL, sessionId)
	resp, err := c.HTTPClient.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("API error: [%s] %s", errResp.Code, errResp.Message)
	}
	return nil
}

func (c *Client) post(url string, body interface{}) (io.ReadCloser, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		resp.Body.Close()
		return nil, fmt.Errorf("API error: [%s] %s (trace_id: %s)", errResp.Code, errResp.Message, errResp.TraceId)
	}

	return resp.Body, nil
}
