package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type capturedRequest struct {
	Method   string
	Path     string
	RawQuery string
	Body     []byte
}

func TestClient_RequestPathsAndQueryParams(t *testing.T) {
	t.Helper()

	var got []capturedRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		got = append(got, capturedRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
			Body:     b,
		})

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/collectors/list":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(CollectorsListResponse{Collectors: []CollectorCandidate{}})
			return
		case r.Method == http.MethodGet && r.URL.Path == "/v1/collectors/queries":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(CollectorsQueriesListResponse{Queries: []CollectorQueryCandidate{}})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/collectors/run":
			w.Header().Set("Content-Type", "application/json")
			// Return query-shaped response if query_id is present, else collector-shaped.
			if strings.Contains(string(b), "\"query_id\"") && !strings.Contains(string(b), "\"query_id\":\"\"") {
				_ = json.NewEncoder(w).Encode(QueryResult{
					DbType:      "postgres",
					CollectorId: "top",
					SourceFile:  "collector-15-top.yaml",
					QueryId:     "q",
					Description: "",
					RenderHint:  map[string]any{},
					Result: ExecuteResponse{
						Type:   "tabular",
						Schema: "",
						Data: DataContent{
							Columns: []ColumnDefinition{},
							Rows:    []map[string]any{},
						},
						Metadata: ResponseMetadata{},
					},
				})
				return
			}

			_ = json.NewEncoder(w).Encode(CollectorResult{
				DbType:      "postgres",
				CollectorId: "top",
				SourceFile:  "collector-15-top.yaml",
				Layers:      map[string]LayerResult{},
				Queries:     map[string]any{},
			})
			return
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/sessions/") && strings.HasSuffix(r.URL.Path, "/samplers"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SamplersListResponse{Samplers: []string{}})
			return
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/sessions/") && strings.Contains(r.URL.Path, "/samplers/"):
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/snapshot") {
				_ = json.NewEncoder(w).Encode(CollectorResult{
					DbType:      "postgres",
					CollectorId: "top",
					SourceFile:  "collector-15-top.yaml",
					Layers:      map[string]LayerResult{},
					Queries:     map[string]any{},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(SamplerStatusResponse{SamplerId: "top", Status: "STOPPED", Reason: ""})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/sessions/") && strings.Contains(r.URL.Path, "/samplers/"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SamplerControlResponse{SamplerId: "top", Status: "RUNNING", Reason: ""})
			return
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/sessions/") && strings.Contains(r.URL.Path, "/samplers/"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SamplerControlResponse{SamplerId: "top", Status: "STOPPED", Reason: ""})
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 250*time.Millisecond)

	// collectors/list
	_, err := c.CollectorsList("sid")
	if err != nil {
		t.Fatalf("CollectorsList error: %v", err)
	}

	// collectors/queries with and without collector_id
	_, err = c.CollectorsQueriesList("sid", "")
	if err != nil {
		t.Fatalf("CollectorsQueriesList(empty) error: %v", err)
	}
	_, err = c.CollectorsQueriesList("sid", "top")
	if err != nil {
		t.Fatalf("CollectorsQueriesList(top) error: %v", err)
	}

	// collectors/run (collector result)
	_, _, err = c.CollectorsRun(&CollectorsRunRequest{SessionId: "sid", CollectorId: "top"})
	if err != nil {
		t.Fatalf("CollectorsRun(collector) error: %v", err)
	}

	// collectors/run (query result)
	_, _, err = c.CollectorsRun(&CollectorsRunRequest{SessionId: "sid", CollectorId: "top", QueryId: "q"})
	if err != nil {
		t.Fatalf("CollectorsRun(query) error: %v", err)
	}

	// samplers endpoints
	_, err = c.SamplersList("sid")
	if err != nil {
		t.Fatalf("SamplersList error: %v", err)
	}
	_, err = c.SamplerStatus("sid", "top")
	if err != nil {
		t.Fatalf("SamplerStatus error: %v", err)
	}
	_, err = c.SamplerSnapshot("sid", "top")
	if err != nil {
		t.Fatalf("SamplerSnapshot error: %v", err)
	}
	_, err = c.SamplerUpsert("sid", "top", &SamplerDefinition{Enabled: ptrBool(true)})
	if err != nil {
		t.Fatalf("SamplerUpsert error: %v", err)
	}
	_, err = c.SamplerDelete("sid", "top")
	if err != nil {
		t.Fatalf("SamplerDelete error: %v", err)
	}

	// Validate key requests
	assertSaw := func(method string, path string, wantQuery url.Values) {
		t.Helper()
		seen := false
		for _, r := range got {
			if r.Method != method || r.Path != path {
				continue
			}
			seen = true
			q, _ := url.ParseQuery(r.RawQuery)

			match := true
			for k, vs := range wantQuery {
				if strings.Join(q[k], ",") != strings.Join(vs, ",") {
					match = false
					break
				}
			}
			if match {
				return
			}
		}
		if !seen {
			t.Fatalf("did not see request %s %s", method, path)
		}
		t.Fatalf("did not see request %s %s with expected query: %v", method, path, wantQuery)
	}

	assertSaw(http.MethodGet, "/v1/collectors/list", url.Values{"session_id": []string{"sid"}})
	assertSaw(http.MethodGet, "/v1/collectors/queries", url.Values{"session_id": []string{"sid"}})
	assertSaw(http.MethodGet, "/v1/collectors/queries", url.Values{"session_id": []string{"sid"}, "collector_id": []string{"top"}})
}

func ptrBool(v bool) *bool {
	return &v
}
