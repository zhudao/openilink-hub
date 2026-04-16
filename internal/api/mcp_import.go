package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/openilink/openilink-hub/internal/store"
)

const maxImportTools = 200

var blockedHeaderKeys = map[string]bool{
	"host":            true,
	"content-type":    true,
	"content-length":  true,
	"transfer-encoding": true,
	"connection":      true,
}

type mcpImportRequest struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

type mcpImportResult struct {
	ServerName    string          `json:"server_name,omitempty"`
	ServerVersion string          `json:"server_version,omitempty"`
	Tools         []store.AppTool `json:"tools"`
	Truncated     bool            `json:"truncated,omitempty"`
}

// importError carries stage + user-facing detail so the frontend can show a
// meaningful message instead of a generic "bad gateway".
type importError struct {
	Stage  string // connect | initialize | list_tools | blocked | timeout | panic
	Detail string
	Err    error
}

func (e *importError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Stage, e.Detail, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Stage, e.Detail)
}

func (e *importError) Unwrap() error { return e.Err }

// handleImportMCP discovers tools from a remote MCP server.
func (s *Server) handleImportMCP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	body := http.MaxBytesReader(w, r.Body, 8*1024)
	var req mcpImportRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		jsonError(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		jsonError(w, "url is required", http.StatusBadRequest)
		return
	}

	u, err := url.ParseRequestURI(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		jsonError(w, "url must be a valid http or https URL", http.StatusBadRequest)
		return
	}

	headers := filterHeaders(req.Headers)

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	result, err := discoverMCPTools(ctx, s.Version, req.URL, headers)
	if err != nil {
		status, msg, stage := classifyImportError(ctx, err)
		slog.Warn("mcp import failed", "url", req.URL, "stage", stage, "status", status, "err", err, "duration_ms", time.Since(start).Milliseconds())
		jsonError(w, msg, status)
		return
	}

	slog.Info("mcp import", "url", req.URL, "server", result.ServerName, "tools", len(result.Tools), "truncated", result.Truncated, "duration_ms", time.Since(start).Milliseconds())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// classifyImportError maps a discovery failure to an HTTP status and a
// user-facing message. Returns (status, message, stage-for-logs).
func classifyImportError(ctx context.Context, err error) (int, string, string) {
	var ie *importError
	if errors.As(err, &ie) {
		switch ie.Stage {
		case "blocked":
			return http.StatusBadRequest, ie.Detail, ie.Stage
		case "timeout":
			return http.StatusGatewayTimeout, ie.Detail, ie.Stage
		case "panic":
			return http.StatusInternalServerError, "MCP client crashed while contacting the server", ie.Stage
		default:
			return http.StatusBadGateway, fmt.Sprintf("%s failed: %s", ie.Stage, ie.Detail), ie.Stage
		}
	}

	if ctx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "MCP server did not respond within 15s", "timeout"
	}
	return http.StatusBadGateway, truncate(err.Error(), 200), "unknown"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func filterHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	filtered := make(map[string]string, len(headers))
	for k, v := range headers {
		lower := stringToLower(k)
		if !blockedHeaderKeys[lower] {
			filtered[k] = v
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func stringToLower(s string) string {
	b := make([]byte, len(s))
	for i := range len(s) {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func discoverMCPTools(ctx context.Context, hubVersion, serverURL string, headers map[string]string) (result *mcpImportResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &importError{Stage: "panic", Detail: fmt.Sprintf("%v", r)}
		}
	}()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext:           ssrfSafeDialContext,
			ResponseHeaderTimeout: 10 * time.Second,
		},
		Timeout: 15 * time.Second,
	}

	// Try Streamable HTTP first (new spec). If the server signals it is a legacy
	// SSE server, fall back to the SSE transport.
	result, err = runDiscovery(ctx, hubVersion, serverURL, headers, httpClient, transportStreamable)
	if err != nil && isLegacySSESignal(err) {
		slog.Info("mcp import: retrying with SSE transport", "url", serverURL)
		result, err = runDiscovery(ctx, hubVersion, serverURL, headers, httpClient, transportSSE)
	}
	return result, err
}

type transportKind int

const (
	transportStreamable transportKind = iota
	transportSSE
)

func runDiscovery(ctx context.Context, hubVersion, serverURL string, headers map[string]string, httpClient *http.Client, kind transportKind) (*mcpImportResult, error) {
	var c *client.Client
	var cerr error

	switch kind {
	case transportSSE:
		opts := []transport.ClientOption{transport.WithHTTPClient(httpClient)}
		if len(headers) > 0 {
			opts = append(opts, transport.WithHeaders(headers))
		}
		c, cerr = client.NewSSEMCPClient(serverURL, opts...)
	default:
		opts := []transport.StreamableHTTPCOption{transport.WithHTTPBasicClient(httpClient)}
		if len(headers) > 0 {
			opts = append(opts, transport.WithHTTPHeaders(headers))
		}
		c, cerr = client.NewStreamableHttpClient(serverURL, opts...)
	}
	if cerr != nil {
		return nil, &importError{Stage: "connect", Detail: "invalid MCP client configuration", Err: cerr}
	}
	defer c.Close()

	if serr := c.Start(ctx); serr != nil {
		return nil, wrapTransportError("connect", serr)
	}

	version := hubVersion
	if version == "" {
		version = "dev"
	}

	initResult, ierr := c.Initialize(ctx, mcp.InitializeRequest{
		Params: mcp.InitializeParams{
			ClientInfo: mcp.Implementation{
				Name:    "OpeniLink Hub",
				Version: version,
			},
			ProtocolVersion: mcp.LATEST_PROTOCOL_VERSION,
		},
	})
	if ierr != nil {
		return nil, wrapTransportError("initialize", ierr)
	}

	result := &mcpImportResult{Tools: []store.AppTool{}}
	if initResult != nil {
		result.ServerName = initResult.ServerInfo.Name
		result.ServerVersion = initResult.ServerInfo.Version
	}

	toolsResult, lerr := c.ListTools(ctx, mcp.ListToolsRequest{})
	if lerr != nil {
		return nil, wrapTransportError("list_tools", lerr)
	}

	for i, t := range toolsResult.Tools {
		if i >= maxImportTools {
			result.Truncated = true
			break
		}

		appTool := store.AppTool{
			Name:        t.Name,
			Description: t.Description,
		}

		params, err := json.Marshal(t.InputSchema)
		if err == nil && len(params) > 0 && string(params) != `{"type":""}` && string(params) != `{"type":"object"}` {
			appTool.Parameters = params
		}

		result.Tools = append(result.Tools, appTool)
	}

	return result, nil
}

// isLegacySSESignal reports whether the error indicates the server speaks the
// legacy SSE transport instead of Streamable HTTP, so discovery should retry.
func isLegacySSESignal(err error) bool {
	if err == nil {
		return false
	}
	low := strings.ToLower(err.Error())
	return strings.Contains(low, "legacy sse")
}

// wrapTransportError classifies a network/protocol error into an importError
// with a concise, user-safe message.
func wrapTransportError(stage string, err error) error {
	msg := err.Error()
	low := strings.ToLower(msg)

	switch {
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(low, "deadline exceeded"):
		return &importError{Stage: "timeout", Detail: "MCP server did not respond within 15s", Err: err}
	case strings.Contains(low, "private/internal"):
		return &importError{Stage: "blocked", Detail: "MCP server resolves to a private/internal IP and was blocked", Err: err}
	case strings.Contains(low, "cannot resolve host"):
		return &importError{Stage: stage, Detail: "cannot resolve MCP server hostname", Err: err}
	case strings.Contains(low, "connection refused"):
		return &importError{Stage: stage, Detail: "connection refused by MCP server", Err: err}
	case strings.Contains(low, "tls") || strings.Contains(low, "x509"):
		return &importError{Stage: stage, Detail: "TLS handshake failed", Err: err}
	case strings.Contains(low, "401") || strings.Contains(low, "unauthorized"):
		return &importError{Stage: stage, Detail: "MCP server rejected credentials (401)", Err: err}
	case strings.Contains(low, "403") || strings.Contains(low, "forbidden"):
		return &importError{Stage: stage, Detail: "MCP server forbade access (403)", Err: err}
	case strings.Contains(low, "404"):
		return &importError{Stage: stage, Detail: "MCP endpoint not found (404) — check the URL path", Err: err}
	case strings.Contains(low, "method not allowed") || strings.Contains(low, "405"):
		return &importError{Stage: stage, Detail: "MCP server does not accept Streamable HTTP transport", Err: err}
	case strings.Contains(low, "legacy sse"):
		return &importError{Stage: stage, Detail: "MCP server uses the legacy SSE transport and the fallback also failed", Err: err}
	default:
		return &importError{Stage: stage, Detail: truncate(msg, 200), Err: err}
	}
}

