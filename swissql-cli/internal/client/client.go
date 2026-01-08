package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Timeout    time.Duration
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
		Timeout: timeout,
	}
}

func (c *Client) Status() error {
	url := fmt.Sprintf("%s/v1/status", c.BaseURL)
	resp, err := c.getWithTimeout(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("API error: status=%d", resp.StatusCode)
	}
	return nil
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

type MetaExplainRequest struct {
	SessionId string `json:"session_id"`
	Sql       string `json:"sql"`
	Analyze   bool   `json:"analyze"`
}

type DriversResponse struct {
	Drivers []DriverEntry `json:"drivers"`
}

type DriversReloadResponse struct {
	Status   string                 `json:"status"`
	Reloaded map[string]interface{} `json:"reloaded"`
}

type DriverEntry struct {
	DbType          string   `json:"db_type"`
	Source          string   `json:"source"`
	DriverClass     string   `json:"driver_class"`
	DriverClasses   []string `json:"driver_classes"`
	JarPaths        []string `json:"jar_paths"`
	JdbcUrlTemplate string   `json:"jdbc_url_template"`
	DefaultPort     *int     `json:"default_port"`
}

func (r *DriversResponse) HasDbType(dbType string) bool {
	if r == nil {
		return false
	}
	needle := strings.ToLower(strings.TrimSpace(dbType))
	if needle == "" {
		return false
	}
	for _, d := range r.Drivers {
		if strings.ToLower(strings.TrimSpace(d.DbType)) == needle {
			return true
		}
	}
	return false
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

type ApiErrorKind string

const (
	ApiErrorKindAPI ApiErrorKind = "api"
	ApiErrorKindDB  ApiErrorKind = "db"
)

type ApiError struct {
	Kind    ApiErrorKind
	Status  int
	Code    string
	Message string
	TraceId string
}

func (e *ApiError) Error() string {
	if e == nil {
		return ""
	}

	prefix := "API error"
	if e.Kind == ApiErrorKindDB {
		prefix = "DB error"
	}

	if e.Code != "" && e.TraceId != "" {
		return fmt.Sprintf("%s: [%s] %s (trace_id: %s)", prefix, e.Code, e.Message, e.TraceId)
	}
	if e.Code != "" {
		return fmt.Sprintf("%s: [%s] %s", prefix, e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s", prefix, e.Message)
}

var leadingErrorPrefixRegex = regexp.MustCompile(`(?i)^(error:\s*)+`)

func sanitizeDbErrorMessage(msg string) string {
	s := strings.TrimSpace(msg)
	s = leadingErrorPrefixRegex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

type AiGenerateRequest struct {
	Prompt        string `json:"prompt"`
	DbType        string `json:"db_type"`
	SessionId     string `json:"session_id,omitempty"`
	ContextMode   string `json:"context_mode,omitempty"`
	ContextLimit  int    `json:"context_limit,omitempty"`
	SchemaContext string `json:"schema_context,omitempty"`
}

type AiGenerateResponse struct {
	Sql         string   `json:"sql"`
	Risk        string   `json:"risk"`
	Explanation string   `json:"explanation"`
	Warnings    []string `json:"warnings"`
	TraceId     string   `json:"trace_id"`
}

type AiContextClearRequest struct {
	SessionId string `json:"session_id"`
}

type AiContextColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AiContextItem struct {
	Sql          string                   `json:"sql"`
	ExecutedAt   string                   `json:"executed_at"`
	Type         string                   `json:"type"`
	Error        string                   `json:"error,omitempty"`
	Columns      []AiContextColumn        `json:"columns,omitempty"`
	SampleRows   []map[string]interface{} `json:"sample_rows,omitempty"`
	Truncated    bool                     `json:"truncated"`
	RowsAffected int                      `json:"rows_affected"`
	DurationMs   int                      `json:"duration_ms"`
}

type AiContextResponse struct {
	SessionId string          `json:"session_id"`
	Items     []AiContextItem `json:"items"`
}

func (c *Client) ValidateSession(sessionId string) error {
	q := url.Values{}
	q.Set("session_id", sessionId)
	urlStr := fmt.Sprintf("%s/v1/sessions/validate?%s", c.BaseURL, q.Encode())

	body, err := c.getProbe(urlStr)
	if err != nil {
		return err
	}
	defer body.Close()
	return nil
}

func (c *Client) MetaDescribe(sessionId string, name string, detail string) (*ExecuteResponse, error) {
	q := url.Values{}
	q.Set("session_id", sessionId)
	q.Set("name", name)
	if strings.TrimSpace(detail) != "" {
		q.Set("detail", detail)
	}
	urlStr := fmt.Sprintf("%s/v1/meta/describe?%s", c.BaseURL, q.Encode())

	body, err := c.get(urlStr)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp ExecuteResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) ReloadDrivers() (*DriversReloadResponse, error) {
	urlStr := fmt.Sprintf("%s/v1/meta/drivers/reload", c.BaseURL)
	body, err := c.post(urlStr, map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp DriversReloadResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) MetaDrivers() (*DriversResponse, error) {
	urlStr := fmt.Sprintf("%s/v1/meta/drivers", c.BaseURL)
	body, err := c.get(urlStr)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp DriversResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) MetaConninfo(sessionId string) (*ExecuteResponse, error) {
	q := url.Values{}
	q.Set("session_id", sessionId)
	urlStr := fmt.Sprintf("%s/v1/meta/conninfo?%s", c.BaseURL, q.Encode())

	body, err := c.get(urlStr)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp ExecuteResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) MetaList(sessionId string, kind string, schema string) (*ExecuteResponse, error) {
	q := url.Values{}
	q.Set("session_id", sessionId)
	q.Set("kind", kind)
	if strings.TrimSpace(schema) != "" {
		q.Set("schema", schema)
	}
	urlStr := fmt.Sprintf("%s/v1/meta/list?%s", c.BaseURL, q.Encode())

	body, err := c.get(urlStr)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp ExecuteResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) MetaExplain(sessionId string, sql string, analyze bool) (*ExecuteResponse, error) {
	urlStr := fmt.Sprintf("%s/v1/meta/explain", c.BaseURL)
	body, err := c.post(urlStr, &MetaExplainRequest{SessionId: sessionId, Sql: sql, Analyze: analyze})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp ExecuteResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) MetaCompletions(sessionId string, kind string, schema string, table string, prefix string) (*ExecuteResponse, error) {
	q := url.Values{}
	q.Set("session_id", sessionId)
	q.Set("kind", kind)
	if strings.TrimSpace(schema) != "" {
		q.Set("schema", schema)
	}
	if strings.TrimSpace(table) != "" {
		q.Set("table", table)
	}
	if strings.TrimSpace(prefix) != "" {
		q.Set("prefix", prefix)
	}
	urlStr := fmt.Sprintf("%s/v1/meta/completions?%s", c.BaseURL, q.Encode())

	body, err := c.get(urlStr)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp ExecuteResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
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

func (c *Client) AiContext(sessionId string, limit int) (*AiContextResponse, error) {
	q := url.Values{}
	q.Set("session_id", sessionId)
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	urlStr := fmt.Sprintf("%s/v1/ai/context?%s", c.BaseURL, q.Encode())

	body, err := c.get(urlStr)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var resp AiContextResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) AiContextClear(sessionId string) error {
	urlStr := fmt.Sprintf("%s/v1/ai/context/clear", c.BaseURL)
	respBody, err := c.post(urlStr, &AiContextClearRequest{SessionId: sessionId})
	if err != nil {
		return err
	}
	defer respBody.Close()
	return nil
}

func (c *Client) AiGenerate(req *AiGenerateRequest) (*AiGenerateResponse, error) {
	url := fmt.Sprintf("%s/v1/ai/generate", c.BaseURL)
	respBody, err := c.post(url, req)
	if err != nil {
		return nil, err
	}
	defer respBody.Close()

	var resp AiGenerateResponse
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

		kind := ApiErrorKindAPI
		msg := errResp.Message
		if errResp.Code == "EXECUTION_ERROR" {
			kind = ApiErrorKindDB
			msg = sanitizeDbErrorMessage(msg)
		}

		return nil, &ApiError{
			Kind:    kind,
			Status:  resp.StatusCode,
			Code:    errResp.Code,
			Message: msg,
			TraceId: errResp.TraceId,
		}
	}

	return resp.Body, nil
}

func (c *Client) getWithTimeout(urlStr string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}

	return c.HTTPClient.Do(req)
}

func (c *Client) getProbe(urlStr string) (io.ReadCloser, error) {
	resp, err := c.getWithTimeout(urlStr)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		resp.Body.Close()
		if errResp.Code != "" {
			kind := ApiErrorKindAPI
			msg := errResp.Message
			if errResp.Code == "EXECUTION_ERROR" {
				kind = ApiErrorKindDB
				msg = sanitizeDbErrorMessage(msg)
			}
			return nil, &ApiError{
				Kind:    kind,
				Status:  resp.StatusCode,
				Code:    errResp.Code,
				Message: msg,
				TraceId: errResp.TraceId,
			}
		}
		return nil, &ApiError{Kind: ApiErrorKindAPI, Status: resp.StatusCode, Message: fmt.Sprintf("status=%d", resp.StatusCode)}
	}

	return resp.Body, nil
}

func (c *Client) get(urlStr string) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		resp.Body.Close()
		if errResp.Code != "" {
			kind := ApiErrorKindAPI
			msg := errResp.Message
			if errResp.Code == "EXECUTION_ERROR" {
				kind = ApiErrorKindDB
				msg = sanitizeDbErrorMessage(msg)
			}
			return nil, &ApiError{
				Kind:    kind,
				Status:  resp.StatusCode,
				Code:    errResp.Code,
				Message: msg,
				TraceId: errResp.TraceId,
			}
		}
		return nil, &ApiError{Kind: ApiErrorKindAPI, Status: resp.StatusCode, Message: fmt.Sprintf("status=%d", resp.StatusCode)}
	}

	return resp.Body, nil
}
