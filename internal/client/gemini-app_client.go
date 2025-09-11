package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/luispater/CLIProxyAPI/internal/auth/gemini"
	"github.com/luispater/CLIProxyAPI/internal/config"
	. "github.com/luispater/CLIProxyAPI/internal/constant"
	"github.com/luispater/CLIProxyAPI/internal/interfaces"
	"github.com/luispater/CLIProxyAPI/internal/registry"
	"github.com/luispater/CLIProxyAPI/internal/translator/translator"
	"github.com/luispater/CLIProxyAPI/internal/util"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	geminiAppBaseURL    = "https://gemini.google.com"
	geminiAppGenerate   = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate"
	geminiAppUpload     = "https://content-push.googleapis.com/upload"
	geminiAppRotate     = "https://accounts.google.com/RotateCookies"
	geminiAppInit       = "https://gemini.google.com/app"
	geminiAppUserAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	geminiAppPushID     = "feeds/mcudyrk2a4khkz"
	geminiAppModelFlash = `[1,null,null,null,"71c2d248d3b102ff",null,null,0,[4]]`
	geminiAppModelPro   = `[1,null,null,null,"4af6c7f5da75d65d",null,null,0,[4]]`
	geminiAppContHint   = "\n(More messages to come, please reply with just 'ok.')"
	geminiAppMaxChars   = 24000
)

// Minimal error codes mapping (from Gemini web responses)
const (
	errUsageLimitExceeded   = 1037
	errModelInconsistent    = 1050
	errModelHeaderInvalid   = 1052
	errIPTemporarilyBlocked = 1060
)

// Typed errors for mapping to HTTP status codes
var (
	errGeminiUsageLimitExceeded = errors.New("usage limit exceeded")
	errGeminiModelInconsistent  = errors.New("model inconsistent")
	errGeminiModelInvalid       = errors.New("model invalid")
	errGeminiTemporarilyBlocked = errors.New("temporarily blocked")
	errGeminiAPI                = errors.New("API error")
)

type GeminiAppClient struct {
    ClientBase
    accessToken   string
    cookies       []*http.Cookie
    tokenFilePath string

    // in-memory conversation store: key -> metadata
    convStore map[string][]string
    convMutex sync.RWMutex

	cookieRotationStarted bool
}

func NewGeminiAppClient(cfg *config.Config, ts *gemini.GeminiAppTokenStorage, tokenFilePath string) (*GeminiAppClient, error) {
	jar, _ := cookiejar.New(nil)
	cookieURL, _ := url.Parse(geminiAppBaseURL)
	cookies := []*http.Cookie{
		{Name: "__Secure-1PSID", Value: ts.Secure1PSID, Domain: ".google.com"},
	}
	if ts.Secure1PSIDTS != "" {
		cookies = append(cookies, &http.Cookie{Name: "__Secure-1PSIDTS", Value: ts.Secure1PSIDTS, Domain: ".google.com"})
	}
	jar.SetCookies(cookieURL, cookies)

	// Build HTTP client with shared proxy handling (supports socks5/http/https)
	httpClient := util.SetProxy(cfg, &http.Client{Jar: jar})

	// Guard substring length for clientID generation
	idPrefix := ts.Secure1PSID
	if len(idPrefix) > 8 {
		idPrefix = idPrefix[:8]
	}
	clientID := fmt.Sprintf("gemini-app-%s-%d", idPrefix, time.Now().UnixNano())
	client := &GeminiAppClient{
		ClientBase: ClientBase{
			RequestMutex:       &sync.Mutex{},
			httpClient:         httpClient,
			cfg:                cfg,
			tokenStorage:       ts,
			modelQuotaExceeded: make(map[string]*time.Time),
		},
		cookies:       cookies,
		tokenFilePath: tokenFilePath,
		convStore:     make(map[string][]string),
	}

	client.InitializeModelRegistry(clientID)
	client.RegisterModels(GEMINI, registry.GetGeminiModels())

    // Try initial access token, but do not fail the whole program if it fails
    if err := client.Init(); err != nil {
        log.Warnf("Gemini App initial token fetch failed for %s: %v. Will retry in background.", client.GetEmail(), err)
        go client.backgroundInitRetry()
    } else {
        client.cookieRotationStarted = true
        go client.startCookieRotation()
    }

	return client, nil
}

// Init initializes the GeminiAppClient
func (c *GeminiAppClient) Init() error {
	return c.refreshAccessToken()
}

func (c *GeminiAppClient) Type() string {
	return GEMINI
}

func (c *GeminiAppClient) Provider() string {
	return GEMINI
}

func (c *GeminiAppClient) CanProvideModel(modelName string) bool {
	return util.InArray([]string{"gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite"}, modelName)
}

func (c *GeminiAppClient) GetEmail() string {
	base := filepath.Base(c.tokenFilePath)
	return strings.TrimSuffix(base, ".json")
}

func (c *GeminiAppClient) SendRawMessage(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
    // Keep a pristine copy for translator context
    originalRequestRawJSON := bytes.Clone(rawJSON)

	// Normalize request into Gemini-style JSON if coming from another handler
	var handlerType string
	if handler, ok := ctx.Value("handler").(interfaces.APIHandler); ok {
		handlerType = handler.HandlerType()
		rawJSON = translator.Request(handlerType, c.Type(), modelName, rawJSON, false)
	}

	// Log upstream API request body for request logger
	if c.cfg.RequestLog {
		if ginContext, ok := ctx.Value("gin").(*gin.Context); ok {
			ginContext.Set("API_REQUEST", rawJSON)
		}
	}

    // Parse messages and inline files (if any)
    messages, files, mimes, err := c.parseMessagesAndFiles(rawJSON)
    if err != nil {
        return nil, &interfaces.ErrorMessage{StatusCode: 400, Error: fmt.Errorf("bad request: %w", err)}
    }
    uploadedFiles, upErr := c.uploadInlineFiles(files, mimes)
    if upErr != nil {
        return nil, upErr
    }

    // Conversation reuse: try to find prior metadata by history prefix ending with assistant/system
    cleaned := sanitizeAssistantMessages(messages)
    meta, reuseIdx := c.findReusableMetadata(modelName, cleaned)

    // Build prompt from remaining messages (suffix);
    // if only one new user message and we have metadata, skip role tags.
    useTags := needRoleTags(messages)
    if reuseIdx > 0 && len(messages[reuseIdx:]) == 1 && strings.ToLower(messages[reuseIdx].Role) == "user" {
        useTags = false
    }
    prompt := buildPrompt(messages[reuseIdx:], useTags, useTags)

    log.Debugf("Use Gemini App account %s for model %s", c.GetEmail(), modelName)
    // Perform generation via the web API
    output, genErr := c.sendWithSplit(ctx, modelName, prompt, meta, uploadedFiles...)
    if genErr != nil {
        log.Errorf("failed to generate content: %v", genErr)
        status := 500
        switch {
        case errors.Is(genErr, errGeminiUsageLimitExceeded), errors.Is(genErr, errGeminiTemporarilyBlocked):
			status = 429
		case errors.Is(genErr, errGeminiModelInconsistent), errors.Is(genErr, errGeminiModelInvalid):
			status = 400
		}
		if status == 429 {
			now := time.Now()
			c.modelQuotaExceeded[modelName] = &now
			c.SetModelQuotaExceeded(modelName)
		}
		return nil, &interfaces.ErrorMessage{StatusCode: status, Error: genErr}
	}

    // Clear quota status on success
    delete(c.modelQuotaExceeded, modelName)
    c.ClearModelQuotaExceeded(modelName)

    // Convert to Gemini API-style JSON, then translate if needed for handler
    gemBytes, errMsg := c.convertOutputToGemini(output, modelName)
    if errMsg != nil {
        return nil, errMsg
    }

    // Log the constructed upstream-like response for request logger
    c.AddAPIResponseData(ctx, gemBytes)

    // Store refreshed conversation metadata for future reuse
    if output != nil && len(output.Metadata) > 0 && len(output.Candidates) > 0 {
        c.storeConversation(modelName, cleaned, output.Candidates[0].Text, output.Metadata)
    }

    if translator.NeedConvert(handlerType, c.Type()) {
        var param any
        out := translator.ResponseNonStream(handlerType, c.Type(), ctx, modelName, originalRequestRawJSON, rawJSON, gemBytes, &param)
        return []byte(out), nil
    }
    return gemBytes, nil
}

func (c *GeminiAppClient) SendRawMessageStream(ctx context.Context, modelName string, rawJSON []byte, alt string) (<-chan []byte, <-chan *interfaces.ErrorMessage) {
	dataChan := make(chan []byte)
	errChan := make(chan *interfaces.ErrorMessage)

	go func() {
		defer close(dataChan)
		defer close(errChan)

		// Keep a pristine copy for translator context
		originalRequestRawJSON := bytes.Clone(rawJSON)

		// Normalize request into Gemini-style JSON if coming from another handler
		var handlerType string
		if handler, ok := ctx.Value("handler").(interfaces.APIHandler); ok {
			handlerType = handler.HandlerType()
			rawJSON = translator.Request(handlerType, c.Type(), modelName, rawJSON, true)
		}

		// Log upstream API request body for request logger
		if c.cfg.RequestLog {
			if ginContext, ok := ctx.Value("gin").(*gin.Context); ok {
				ginContext.Set("API_REQUEST", rawJSON)
			}
		}

		// Start keepalive: immediately emit an initial role/empty chunk and then periodic empty chunks
		// until the first real chunk is available. This prevents 60s idle timeouts on proxies.
		var kaOnce sync.Once
		stopKA := make(chan struct{})
		stopKeepalive := func() { kaOnce.Do(func() { close(stopKA) }) }
		sendKeepalive := func() {
			var param any
			if translator.NeedConvert(handlerType, c.Type()) && handlerType != GEMINI {
				lines := translator.Response(handlerType, c.Type(), ctx, modelName, originalRequestRawJSON, rawJSON, buildGeminiChunk(modelName, "", false, "", false, false), &param)
				for _, l := range lines {
					if l != "" {
						dataChan <- []byte(l)
					}
				}
			} else {
				dataChan <- buildGeminiChunk(modelName, "", false, "", false, false)
			}
		}
		// Send initial keepalive chunk right away
		sendKeepalive()
		// Ticker for periodic keepalive before first real chunk
		go func() {
			ticker := time.NewTicker(12 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-stopKA:
					return
				case <-ticker.C:
					sendKeepalive()
				}
			}
		}()

        // Build messages and upload inline files if any
        messages, files, mimes, err := c.parseMessagesAndFiles(rawJSON)
        if err != nil {
            stopKeepalive()
            errChan <- &interfaces.ErrorMessage{StatusCode: 400, Error: fmt.Errorf("bad request: %w", err)}
            return
        }
        uploadedFiles, upErr := c.uploadInlineFiles(files, mimes)
        if upErr != nil {
            stopKeepalive()
            errChan <- upErr
            return
        }

        cleaned := sanitizeAssistantMessages(messages)
        meta, reuseIdx := c.findReusableMetadata(modelName, cleaned)
        useTags := needRoleTags(messages)
        if reuseIdx > 0 && len(messages[reuseIdx:]) == 1 && strings.ToLower(messages[reuseIdx].Role) == "user" {
            useTags = false
        }
        prompt := buildPrompt(messages[reuseIdx:], useTags, useTags)

        log.Debugf("Use Gemini App account %s for model %s", c.GetEmail(), modelName)
        // Call upstream Gemini App with splitting if needed
        output, genErr := c.sendWithSplit(ctx, modelName, prompt, meta, uploadedFiles...)
        if genErr != nil {
            status := 500
            switch {
            case errors.Is(genErr, errGeminiUsageLimitExceeded), errors.Is(genErr, errGeminiTemporarilyBlocked):
                status = 429
            case errors.Is(genErr, errGeminiModelInconsistent), errors.Is(genErr, errGeminiModelInvalid):
                status = 400
            }
            if status == 429 {
                now := time.Now()
                c.modelQuotaExceeded[modelName] = &now
                c.SetModelQuotaExceeded(modelName)
            }
            stopKeepalive()
            errChan <- &interfaces.ErrorMessage{StatusCode: status, Error: genErr}
            return
        }

        // Clear quota status on success
        delete(c.modelQuotaExceeded, modelName)
        c.ClearModelQuotaExceeded(modelName)

        // Convert to Gemini response JSON first
        gemBytes, errMsg := c.convertOutputToGemini(output, modelName)
        if errMsg != nil {
            stopKeepalive()
            errChan <- errMsg
            return
        }

        // Log upstream-like response
        c.AddAPIResponseData(ctx, gemBytes)

        if output != nil && len(output.Metadata) > 0 && len(output.Candidates) > 0 {
            c.storeConversation(modelName, cleaned, output.Candidates[0].Text, output.Metadata)
        }

        // If handler expects another format (e.g. OpenAI/Claude/GeminiCLI),
        // simulate streaming by emitting Gemini-shaped chunks and translating each chunk.
        if translator.NeedConvert(handlerType, c.Type()) && handlerType != GEMINI {
            stopKeepalive()
            simulateTranslatedStreaming(ctx, dataChan, handlerType, c.Type(), modelName, originalRequestRawJSON, rawJSON, output)
            return
        }
        // Otherwise (Gemini->Gemini passthrough), simulate streaming like docs/gemini-fastapi
        // by splitting the final text into small chunks and emitting Gemini-shaped JSON.
        stopKeepalive()
        simulateGeminiStreaming(dataChan, modelName, output)
    }()

	return dataChan, errChan
}

// chunkByRunes splits a string into rune-safe chunks of up to size runes.
func chunkByRunes(s string, size int) []string {
	if size <= 0 {
		return []string{s}
	}
	chunks := make([]string, 0, (len(s)/size)+1)
	var buf strings.Builder
	count := 0
	for _, r := range s {
		buf.WriteRune(r)
		count++
		if count >= size {
			chunks = append(chunks, buf.String())
			buf.Reset()
			count = 0
		}
	}
	if buf.Len() > 0 {
		chunks = append(chunks, buf.String())
	}
	if len(chunks) == 0 {
		return []string{""}
	}
	return chunks
}

// simulateGeminiStreaming emits SSE-friendly JSON chunks in a Gemini-like shape.
// It mirrors docs/gemini-fastapi behavior: send small text pieces as sequential events
// and include finish information in the final event. Token usage is not available here,
// so it falls back to zeros.
// buildGeminiChunk builds a Gemini-like JSON chunk for streaming.
func buildGeminiChunk(modelName, text string, thought bool, finish string, includeFinish, includeUsage bool) []byte {
	parts := []map[string]any{}
	if text != "" {
		part := map[string]any{"text": text}
		if thought {
			part["thought"] = true
		}
		parts = append(parts, part)
	}
	resp := map[string]any{
		"candidates": []any{
			map[string]any{
				"content": map[string]any{"parts": parts, "role": "model"},
				"index":   0,
			},
		},
		"createTime":   time.Now().Format(time.RFC3339Nano),
		"responseId":   fmt.Sprintf("gemini-app-%d", time.Now().UnixNano()),
		"modelVersion": modelName,
	}
	if includeFinish {
		cand := resp["candidates"].([]any)[0].(map[string]any)
		cand["finishReason"] = finish
	}
	if includeUsage {
		resp["usageMetadata"] = map[string]any{
			"promptTokenCount":     0,
			"candidatesTokenCount": 0,
			"totalTokenCount":      0,
		}
	}
	b, _ := json.Marshal(resp)
	return b
}

func simulateGeminiStreaming(out chan<- []byte, modelName string, output *ModelOutput) {
	if output == nil || len(output.Candidates) == 0 {
		// Nothing to stream
		return
	}

	// Use a conservative small chunk size to deliver a smooth stream.
	// Count by runes to avoid splitting multi-byte characters.
	const chunkSize = 32

	// Send an initial role event (empty delta) to align with some client expectations
	out <- buildGeminiChunk(modelName, "", false, "", false, false)

	// First, stream reasoning/thoughts as dedicated thought chunks if present.
	if t := strings.TrimSpace(output.Candidates[0].Thoughts); t != "" {
		for _, ch := range chunkByRunes(t, chunkSize) {
			out <- buildGeminiChunk(modelName, ch, true, "", false, false)
		}
	}

	// Then stream the actual assistant text.
	text := output.Candidates[0].Text
	textChunks := chunkByRunes(text, chunkSize)

	for i := 0; i < len(textChunks); i++ {
		last := i == len(textChunks)-1
		if last {
			out <- buildGeminiChunk(modelName, textChunks[i], false, "", false, false)
			out <- buildGeminiChunk(modelName, "", false, "STOP", true, true)
		} else {
			out <- buildGeminiChunk(modelName, textChunks[i], false, "", false, false)
		}
	}
}

// simulateTranslatedStreaming splits the final output into Gemini-shaped chunks,
// translates each chunk into the target handler format (e.g., OpenAI Chat Completions),
// and pushes them to dataChan one by one.
func simulateTranslatedStreaming(
	ctx context.Context,
	dataChan chan<- []byte,
	handlerType string,
	providerType string,
	modelName string,
	originalRequestRawJSON []byte,
	requestRawJSON []byte,
	output *ModelOutput,
) {
	if output == nil || len(output.Candidates) == 0 {
		return
	}

	// Prepare chunkers similar to simulateGeminiStreaming, but each chunk is
	// passed through the translator for the target handler.
	const chunkSize = 32

	var param any
	// Optionally send an initial empty chunk to establish role in some translators
	initLines := translator.Response(handlerType, providerType, ctx, modelName, originalRequestRawJSON, requestRawJSON, buildGeminiChunk(modelName, "", false, "", false, false), &param)
	for _, l := range initLines {
		if l != "" {
			dataChan <- []byte(l)
		}
	}

	// Stream thought chunks first (as reasoning parts), then main text chunks.
	if t := strings.TrimSpace(output.Candidates[0].Thoughts); t != "" {
		for _, ch := range chunkByRunes(t, chunkSize) {
			lines := translator.Response(handlerType, providerType, ctx, modelName, originalRequestRawJSON, requestRawJSON, buildGeminiChunk(modelName, ch, true, "", false, false), &param)
			for _, l := range lines {
				if l != "" {
					dataChan <- []byte(l)
				}
			}
		}
	}

	text := output.Candidates[0].Text
	textChunks := chunkByRunes(text, chunkSize)
	for i := 0; i < len(textChunks); i++ {
		last := i == len(textChunks)-1
		if last {
			lines := translator.Response(handlerType, providerType, ctx, modelName, originalRequestRawJSON, requestRawJSON, buildGeminiChunk(modelName, textChunks[i], false, "", false, false), &param)
			for _, l := range lines {
				if l != "" {
					dataChan <- []byte(l)
				}
			}

			endLines := translator.Response(handlerType, providerType, ctx, modelName, originalRequestRawJSON, requestRawJSON, buildGeminiChunk(modelName, "", false, "stop", true, true), &param)
			for _, l := range endLines {
				if l != "" {
					dataChan <- []byte(l)
				}
			}
		} else {
			lines := translator.Response(handlerType, providerType, ctx, modelName, originalRequestRawJSON, requestRawJSON, buildGeminiChunk(modelName, textChunks[i], false, "", false, false), &param)
			for _, l := range lines {
				if l != "" {
					dataChan <- []byte(l)
				}
			}
		}
	}
}

// mimeToExt maps common MIME types to file extensions.
// Falls back to .png which works for most simple images.
func mimeToExt(mimes []string, i int) string {
	if i < len(mimes) {
		switch strings.ToLower(mimes[i]) {
		case "image/png":
			return ".png"
		case "image/jpeg", "image/jpg":
			return ".jpg"
		case "image/webp":
			return ".webp"
		case "image/gif":
			return ".gif"
		case "image/bmp":
			return ".bmp"
		case "image/heic":
			return ".heic"
		case "application/pdf":
			return ".pdf"
		}
	}
	return ".png"
}

func (c *GeminiAppClient) SendRawTokenCount(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
	// No web endpoint for token counting; return a minimal Gemini-like usage structure
	return []byte(`{"totalTokens":0}`), nil
}

func (c *GeminiAppClient) SaveTokenToFile() error {
	ts := c.tokenStorage.(*gemini.GeminiAppTokenStorage)
	log.Infof("Saving Gemini App credentials to %s", c.tokenFilePath)
	return ts.SaveTokenToFile(c.tokenFilePath)
}

func (c *GeminiAppClient) IsModelQuotaExceeded(model string) bool {
	if t, ok := c.modelQuotaExceeded[model]; ok {
		return time.Since(*t) <= 30*time.Minute
	}
	return false
}

func (c *GeminiAppClient) GetUserAgent() string { return geminiAppUserAgent }

// GetRequestMutex returns nil to opt-out of handler-level per-client locking.
// This matches other clients in the codebase and prevents potential deadlocks
// if a long-lived stream holds the lock beyond expected scopes.
func (c *GeminiAppClient) GetRequestMutex() *sync.Mutex {
	return nil
}

func (c *GeminiAppClient) RefreshTokens(ctx context.Context) error {
	return c.refreshAccessToken()
}

func (c *GeminiAppClient) rotateCookies() error {
	req, _ := http.NewRequest("POST", geminiAppRotate, strings.NewReader(`[000,"-0000000000000000000"]`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Debugf("Cookies rotated successfully for %s", c.GetEmail())
		// The new cookies are automatically added to the jar
		cookieURL, _ := url.Parse(geminiAppBaseURL)
		c.cookies = c.httpClient.Jar.Cookies(cookieURL)

		// Update token storage and save to file
		ts := c.tokenStorage.(*gemini.GeminiAppTokenStorage)
		for _, cookie := range c.cookies {
			if cookie.Name == "__Secure-1PSID" {
				ts.Secure1PSID = cookie.Value
			} else if cookie.Name == "__Secure-1PSIDTS" {
				ts.Secure1PSIDTS = cookie.Value
			}
		}
		if err := c.SaveTokenToFile(); err != nil {
			log.Errorf("Failed to save rotated cookies for %s: %v", c.GetEmail(), err)
		}
	} else {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("Failed to rotate cookies for %s, status: %d, body: %s", c.GetEmail(), resp.StatusCode, string(body))
	}
	return nil
}

func (c *GeminiAppClient) startCookieRotation() {
    ticker := time.NewTicker(2 * time.Hour)
    defer ticker.Stop()

	for {
		<-ticker.C
		log.Debugf("Rotating cookies for %s...", c.GetEmail())
		if err := c.rotateCookies(); err != nil {
			log.Errorf("Error rotating cookies for %s: %v", c.GetEmail(), err)
		}
	}
}

func (c *GeminiAppClient) refreshAccessToken() error {
	req, _ := http.NewRequest("GET", geminiAppInit, nil)
	c.setHeaders(req, "")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get access token, status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	re := regexp.MustCompile(`"SNlM0e":"(.*?)"`)
	matches := re.FindStringSubmatch(string(body))
	if len(matches) > 1 {
		c.accessToken = matches[1]
		c.cookies = c.httpClient.Jar.Cookies(req.URL)
		ts := c.tokenStorage.(*gemini.GeminiAppTokenStorage)
		for _, cookie := range c.cookies {
			if cookie.Name == "__Secure-1PSID" {
				ts.Secure1PSID = cookie.Value
			} else if cookie.Name == "__Secure-1PSIDTS" {
				ts.Secure1PSIDTS = cookie.Value
			}
		}
		if err := c.SaveTokenToFile(); err != nil {
			log.Errorf("Failed to save refreshed cookies for %s: %v", c.GetEmail(), err)
		}
		return nil
	}

	log.Errorf("access token (SNlM0e) not found in response body from %s", geminiAppInit)
	return fmt.Errorf("access token not found")
}

func (c *GeminiAppClient) setHeaders(req *http.Request, modelName string) {
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	req.Header.Set("Host", "gemini.google.com")
	req.Header.Set("Origin", "https://gemini.google.com")
	req.Header.Set("Referer", "https://gemini.google.com/")
	req.Header.Set("User-Agent", geminiAppUserAgent)
	req.Header.Set("X-Same-Domain", "1")

	modelHeader := geminiAppModelPro
	if modelName == "gemini-2.5-flash" || modelName == "gemini-2.5-flash-lite" {
		modelHeader = geminiAppModelFlash
	}
	req.Header.Set("x-goog-ext-525001261-jspb", modelHeader)
}

func (c *GeminiAppClient) extractRequestData(rawJSON []byte) (string, [][]byte, []string, error) {
    msgs, files, mimes, err := c.parseMessagesAndFiles(rawJSON)
    if err != nil {
        return "", nil, nil, err
    }
    useTags := needRoleTags(msgs)
    prompt := buildPrompt(msgs, useTags, useTags)
    return prompt, files, mimes, nil
}

func (c *GeminiAppClient) backgroundInitRetry() {
    backoffs := []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second, 1 * time.Minute, 2 * time.Minute, 5 * time.Minute}
    i := 0
    for {
        if err := c.refreshAccessToken(); err == nil {
            log.Infof("Gemini App token recovered for %s", c.GetEmail())
            if !c.cookieRotationStarted {
                c.cookieRotationStarted = true
                go c.startCookieRotation()
            }
            return
        }
        d := backoffs[i]
        if i < len(backoffs)-1 {
            i++
        }
        time.Sleep(d)
    }
}

type roleText struct {
    Role string
    Text string
}

func (c *GeminiAppClient) parseMessagesAndFiles(rawJSON []byte) ([]roleText, [][]byte, []string, error) {
    var messages []roleText
    var files [][]byte
    var mimes []string

    contents := gjson.GetBytes(rawJSON, "contents")
    if contents.Exists() {
        contents.ForEach(func(_, content gjson.Result) bool {
            role := strings.ToLower(content.Get("role").String())
            var b strings.Builder
            content.Get("parts").ForEach(func(_, part gjson.Result) bool {
                if text := part.Get("text"); text.Exists() {
                    if b.Len() > 0 {
                        b.WriteString("\n")
                    }
                    b.WriteString(text.String())
                }
                if inlineData := part.Get("inlineData"); inlineData.Exists() {
                    data := inlineData.Get("data").String()
                    if data != "" {
                        if dec, err := base64.StdEncoding.DecodeString(data); err == nil {
                            files = append(files, dec)
                            m := inlineData.Get("mime_type").String()
                            mimes = append(mimes, m)
                        }
                    }
                }
                return true
            })
            messages = append(messages, roleText{Role: role, Text: b.String()})
            return true
        })
    }
    return messages, files, mimes, nil
}

func needRoleTags(msgs []roleText) bool {
    for _, m := range msgs {
        if strings.ToLower(m.Role) != "user" {
            return true
        }
    }
    return false
}

func addRoleTag(role, content string, unclose bool) string {
    if role == "" {
        role = "user"
    }
    if unclose {
        return "<|im_start|>" + role + "\n" + content
    }
    return "<|im_start|>" + role + "\n" + content + "\n<|im_end|>"
}

func buildPrompt(msgs []roleText, tagged bool, appendAssistant bool) string {
    if len(msgs) == 0 {
        if tagged && appendAssistant {
            return addRoleTag("assistant", "", true)
        }
        return ""
    }
    if !tagged {
        var sb strings.Builder
        for i, m := range msgs {
            if i > 0 {
                sb.WriteString("\n")
            }
            sb.WriteString(m.Text)
        }
        return sb.String()
    }
    var sb strings.Builder
    for _, m := range msgs {
        sb.WriteString(addRoleTag(m.Role, m.Text, false))
        sb.WriteString("\n")
    }
    if appendAssistant {
        sb.WriteString(addRoleTag("assistant", "", true))
    }
    return strings.TrimSpace(sb.String())
}

var reThink = regexp.MustCompile(`(?s)^\s*<think>.*?</think>\s*`)

func removeThinkTags(s string) string {
    return strings.TrimSpace(reThink.ReplaceAllString(s, ""))
}

func sanitizeAssistantMessages(msgs []roleText) []roleText {
    out := make([]roleText, 0, len(msgs))
    for _, m := range msgs {
        if strings.ToLower(m.Role) == "assistant" {
            out = append(out, roleText{Role: m.Role, Text: removeThinkTags(m.Text)})
        } else {
            out = append(out, m)
        }
    }
    return out
}

func (c *GeminiAppClient) conversationKey(modelName string, msgs []roleText) string {
    norm := make([]map[string]string, 0, len(msgs))
    for _, m := range msgs {
        t := m.Text
        if strings.ToLower(m.Role) == "assistant" {
            t = removeThinkTags(t)
        }
        norm = append(norm, map[string]string{"r": strings.ToLower(m.Role), "t": t})
    }
    b, _ := json.Marshal(norm)
    sum := sha256.Sum256(b)
    return fmt.Sprintf("%s|%s|%s", modelName, c.GetEmail(), hex.EncodeToString(sum[:]))
}

func (c *GeminiAppClient) findReusableMetadata(modelName string, msgs []roleText) ([]string, int) {
    if len(msgs) < 2 {
        return nil, 0
    }
    for end := len(msgs); end >= 2; end-- {
        if r := strings.ToLower(msgs[end-1].Role); r != "assistant" && r != "system" {
            continue
        }
        key := c.conversationKey(modelName, msgs[:end])
        c.convMutex.RLock()
        meta, ok := c.convStore[key]
        c.convMutex.RUnlock()
        if ok && len(meta) > 0 {
            return meta, end
        }
    }
    return nil, 0
}

func (c *GeminiAppClient) storeConversation(modelName string, msgs []roleText, assistantReply string, metadata []string) {
    all := make([]roleText, 0, len(msgs)+1)
    all = append(all, msgs...)
    all = append(all, roleText{Role: "assistant", Text: removeThinkTags(assistantReply)})
    key := c.conversationKey(modelName, all)
    c.convMutex.Lock()
    c.convStore[key] = metadata
    c.convMutex.Unlock()
}

func (c *GeminiAppClient) sendWithSplit(ctx context.Context, modelName, prompt string, metadata []string, files ...*UploadedFile) (*ModelOutput, error) {
    rlen := func(s string) int { return len([]rune(s)) }
    if rlen(prompt) <= geminiAppMaxChars {
        return c.generateContent(ctx, modelName, prompt, metadata, "", files...)
    }
    hintLen := rlen(geminiAppContHint)
    chunkSize := geminiAppMaxChars - hintLen
    if chunkSize <= 0 {
        chunkSize = geminiAppMaxChars
    }
    chunks := chunkByRunes(prompt, chunkSize)
    if len(chunks) == 0 {
        return c.generateContent(ctx, modelName, prompt, metadata, "", files...)
    }
    currentMeta := metadata
    for i := 0; i < len(chunks)-1; i++ {
        tmpOut, err := c.generateContent(ctx, modelName, chunks[i]+geminiAppContHint, currentMeta, "")
        if err != nil {
            return nil, err
        }
        if tmpOut != nil && len(tmpOut.Metadata) > 0 {
            currentMeta = tmpOut.Metadata
        }
    }
    out, err := c.generateContent(ctx, modelName, chunks[len(chunks)-1], currentMeta, "", files...)
    if err != nil {
        return nil, err
    }
    return out, nil
}

func (c *GeminiAppClient) uploadFile(filePath string) (*UploadedFile, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	var requestBody bytes.Buffer
	multipartWriter := multipart.NewWriter(&requestBody)
	formFileWriter, err := multipartWriter.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err := io.Copy(formFileWriter, file); err != nil {
		return nil, fmt.Errorf("failed to copy file to form: %w", err)
	}
	if err := multipartWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	req, err := http.NewRequest("POST", geminiAppUpload, &requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload request: %w", err)
	}

	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	req.Header.Set("Push-ID", geminiAppPushID)

	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(requestBody.Bytes())), nil
	}

	// Use a local client to preserve POST + body across redirects; inherit proxy transport
	localClient := &http.Client{
		Transport: c.httpClient.Transport,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) > 0 {
				r.Method = via[0].Method
				r.Header = via[0].Header.Clone()
				if via[0].GetBody != nil {
					if body, err := via[0].GetBody(); err == nil {
						r.Body = body
					} else {
						r.Body = io.NopCloser(bytes.NewReader(requestBody.Bytes()))
					}
				} else {
					r.Body = io.NopCloser(bytes.NewReader(requestBody.Bytes()))
				}
			}
			return nil
		},
		Jar:     c.httpClient.Jar,
		Timeout: c.httpClient.Timeout,
	}

	resp, err := localClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute upload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read upload response body: %w", err)
	}
	identifier := strings.TrimSpace(string(responseBody))

	return &UploadedFile{
		Identifier: []string{identifier},
		FileName:   filepath.Base(filePath),
	}, nil
}

// uploadInlineFiles writes inline file bytes to temporary files, uploads them, and returns identifiers.
func (c *GeminiAppClient) uploadInlineFiles(files [][]byte, mimes []string) ([]*UploadedFile, *interfaces.ErrorMessage) {
	if len(files) == 0 {
		return nil, nil
	}
	uploaded := make([]*UploadedFile, 0, len(files))
	for i, data := range files {
		ext := mimeToExt(mimes, i)
		f, err := os.CreateTemp("", "gemini-upload-*"+ext)
		if err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to create temp file: %w", err)}
		}
		if _, err = f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to write temp file: %w", err)}
		}
		if err = f.Close(); err != nil {
			_ = os.Remove(f.Name())
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to close temp file: %w", err)}
		}
		up, err := c.uploadFile(f.Name())
		_ = os.Remove(f.Name())
		if err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to upload file: %w", err)}
		}
		uploaded = append(uploaded, up)
	}
	return uploaded, nil
}

func (c *GeminiAppClient) generateContent(ctx context.Context, modelName, prompt string, metadata []string, gemID string, files ...*UploadedFile) (*ModelOutput, error) {
    // Ensure access token exists; try to refresh if missing
    if strings.TrimSpace(c.accessToken) == "" {
        if err := c.refreshAccessToken(); err != nil {
            return nil, fmt.Errorf("access token unavailable: %w", err)
        }
    }
	var innerPayload []interface{}
	if len(files) > 0 {
		var fileList []interface{}
		for _, file := range files {
			fileList = append(fileList, []interface{}{file.Identifier, file.FileName})
		}
		innerPayload = []interface{}{prompt, 0, nil, fileList}
	} else {
		innerPayload = []interface{}{prompt}
	}

    stringifiedPayload := []interface{}{innerPayload, nil, nil}
    if len(metadata) > 0 {
        stringifiedPayload[2] = metadata
    }
    if gemID != "" {
        for i := 0; i < 16; i++ {
            stringifiedPayload = append(stringifiedPayload, nil)
        }
        stringifiedPayload = append(stringifiedPayload, gemID)
    }

	jsonPayload, err := json.Marshal(stringifiedPayload)
	if err != nil {
		return nil, err
	}

    fReq, err := json.Marshal([]interface{}{nil, string(jsonPayload)})
    if err != nil {
        return nil, err
    }

    data := url.Values{}
    data.Set("at", c.accessToken)
    data.Set("f.req", string(fReq))

    var responseData [][]interface{}

	req, err := http.NewRequestWithContext(ctx, "POST", geminiAppGenerate, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	c.setHeaders(req, modelName)

    // Logging moved to call sites to avoid repeated logs on split/retry

    resp, err := c.httpClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        // Try one refresh-and-retry in case token got invalidated
        body, _ := io.ReadAll(resp.Body)
        c.AddAPIResponseData(ctx, body)
        if err := c.refreshAccessToken(); err == nil {
            req2, err2 := http.NewRequestWithContext(ctx, "POST", geminiAppGenerate, strings.NewReader(data.Encode()))
            if err2 != nil {
                return nil, err2
            }
            c.setHeaders(req2, modelName)
            resp2, err2 := c.httpClient.Do(req2)
            if err2 == nil {
                defer resp2.Body.Close()
                if resp2.StatusCode == http.StatusOK {
                    body2, err2 := io.ReadAll(resp2.Body)
                    if err2 != nil {
                        return nil, err2
                    }
                    c.AddAPIResponseData(ctx, body2)
                    responseDataRetry, perr := func(b []byte) ([][]interface{}, error) {
                        lines := strings.Split(string(b), "\n")
                        var rd [][]interface{}
                        for _, line := range lines {
                            if strings.HasPrefix(line, "[[") {
                                if err := json.Unmarshal([]byte(line), &rd); err == nil {
                                    return rd, nil
                                }
                            }
                        }
                        return nil, fmt.Errorf("failed to find valid JSON data in response: %s", string(b))
                    }(body2)
                    if perr != nil {
                        return nil, perr
                    }
                    // assign and continue parsing with unified logic below
                    responseData = responseDataRetry
                }
            }
        }
        return nil, fmt.Errorf("API request failed with status code %d: %s", resp.StatusCode, string(body))
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    // Log upstream API response body for request logger
    c.AddAPIResponseData(ctx, body)

    parseResp := func(b []byte) ([][]interface{}, error) {
        lines := strings.Split(string(b), "\n")
        var rd [][]interface{}
        for _, line := range lines {
            if strings.HasPrefix(line, "[[") {
                if err := json.Unmarshal([]byte(line), &rd); err == nil {
                    return rd, nil
                }
            }
        }
        return nil, fmt.Errorf("failed to find valid JSON data in response: %s", string(b))
    }
    rd, err := parseResp(body)
    if err != nil {
        return nil, err
    }
    responseData = rd

    var textBuilder strings.Builder
    var thoughtsBuilder strings.Builder
    var respMetadata []string
    foundAnyText := false
    for _, part := range responseData {
        if len(part) < 3 {
            continue
        }
        mainPartStr, ok := part[2].(string)
        if !ok {
            continue
        }
        var mainPart []interface{}
        if err := json.Unmarshal([]byte(mainPartStr), &mainPart); err != nil {
            continue
        }

        if len(mainPart) > 1 && mainPart[1] != nil && respMetadata == nil {
            if mdSlice, ok := mainPart[1].([]interface{}); ok {
                tmp := make([]string, 0, len(mdSlice))
                for _, v := range mdSlice {
                    if s, ok := v.(string); ok {
                        tmp = append(tmp, s)
                    }
                }
                if len(tmp) > 0 {
                    respMetadata = tmp
                }
            }
        }

        if len(mainPart) > 4 && mainPart[4] != nil {
            candidatesData, _ := mainPart[4].([]interface{})
            for _, candData := range candidatesData {
                candList, _ := candData.([]interface{})
                if len(candList) > 1 {
					// Extract thoughts at [37][0][0] if present
					if len(candList) > 37 {
						if arr1, ok := candList[37].([]interface{}); ok && len(arr1) > 0 {
							if arr2, ok2 := arr1[0].([]interface{}); ok2 && len(arr2) > 0 {
								if ts, ok3 := arr2[0].(string); ok3 && ts != "" {
									thoughtsBuilder.WriteString(ts)
									if !strings.HasSuffix(ts, "\n\n") {
										thoughtsBuilder.WriteString("\n\n")
									}
								}
							}
						}
					}
					textSlice, _ := candList[1].([]interface{})
					if len(textSlice) > 0 {
						text, _ := textSlice[0].(string)
						textBuilder.WriteString(text)
						if text != "" {
							foundAnyText = true
						}
					}
				}
			}
		}
	}

	// Minimal error code mapping if no candidate text was found
	if !foundAnyText {
		if errCode, errGetCode := safeGetFloat([][][]interface{}{responseData}, 0, 5, 2, 0, 1); errGetCode == nil {
			switch int(errCode) {
			case errUsageLimitExceeded:
				return nil, errGeminiUsageLimitExceeded
			case errModelInconsistent:
				return nil, errGeminiModelInconsistent
			case errModelHeaderInvalid:
				return nil, errGeminiModelInvalid
			case errIPTemporarilyBlocked:
				return nil, errGeminiTemporarilyBlocked
			default:
				return nil, fmt.Errorf("API error with code: %d", int(errCode))
			}
		}
		return nil, errGeminiAPI
	}

    return &ModelOutput{
        Candidates: []Candidate{
            {Text: textBuilder.String(), Thoughts: strings.TrimSpace(thoughtsBuilder.String())},
        },
        Metadata: respMetadata,
    }, nil
}

// Helper functions for minimal nested slice traversal (error code extraction)
func safeGetFloat(data [][][]interface{}, indices ...int) (float64, error) {
	var current interface{} = data
	for i, index := range indices {
		switch c := current.(type) {
		case [][][]interface{}:
			if index >= len(c) {
				return 0, fmt.Errorf("index %d out of bounds for [][][]interface{}", index)
			}
			current = c[index]
		case [][]interface{}:
			if index >= len(c) {
				return 0, fmt.Errorf("index %d out of bounds for [][]interface{}", index)
			}
			current = c[index]
		case []interface{}:
			if index >= len(c) {
				return 0, fmt.Errorf("index %d out of bounds for []interface{}", index)
			}
			current = c[index]
		default:
			return 0, fmt.Errorf("unexpected type at depth %d: %T", i, current)
		}
	}
	f, ok := current.(float64)
	if !ok {
		return 0, fmt.Errorf("final value is not a float64, but %T", current)
	}
	return f, nil
}

// convertOutputToGemini converts our simplified ModelOutput to a Gemini API-like JSON.
// This enables downstream translators to convert to other formats (e.g., OpenAI) when needed.
func (c *GeminiAppClient) convertOutputToGemini(output *ModelOutput, modelName string) ([]byte, *interfaces.ErrorMessage) {
	if output == nil || len(output.Candidates) == 0 {
		return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("empty output")}
	}

	parts := make([]map[string]any, 0, 2)
	// Include reasoning/thoughts as a dedicated part if present
	if t := strings.TrimSpace(output.Candidates[0].Thoughts); t != "" {
		parts = append(parts, map[string]any{"text": t, "thought": true})
	}
	// Main assistant content
	parts = append(parts, map[string]any{"text": output.Candidates[0].Text})

	resp := map[string]any{
		"candidates": []any{
			map[string]any{
				"content": map[string]any{
					"parts": parts,
					"role":  "model",
				},
				"finishReason": "STOP",
				"index":        0,
			},
		},
		// Provide timestamps and IDs similar to Gemini API fields some translators use
		"createTime":   time.Now().Format(time.RFC3339Nano),
		"responseId":   fmt.Sprintf("gemini-app-%d", time.Now().UnixNano()),
		"modelVersion": modelName,
		"usageMetadata": map[string]any{
			"promptTokenCount":     0,
			"candidatesTokenCount": 0,
			"totalTokenCount":      0,
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to marshal gemini response: %w", err)}
	}
	return b, nil
}

// Dummy structs to represent the output from the gemini-app logic
type ModelOutput struct {
	Candidates []Candidate
	Metadata   []string
}

type Candidate struct {
	Text     string
	Thoughts string
}

type UploadedFile struct {
	Identifier []string
	FileName   string
}
