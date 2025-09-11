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
    "net/http"
    "net/http/cookiejar"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "sync"
    "time"

    "github.com/gin-gonic/gin"
    gemweb "github.com/luispater/CLIProxyAPI/internal/api/geminiwebapi"
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

type GeminiWebClient struct {
    ClientBase
    // Gemini Web API client
    gwc           *gemweb.GeminiClient
    tokenFilePath string

    // in-memory conversation store: key -> metadata
    convStore map[string][]string
    convMutex sync.RWMutex

    cookieRotationStarted bool
    cookiePersistCancel   context.CancelFunc
}

func NewGeminiWebClient(cfg *config.Config, ts *gemini.GeminiAppTokenStorage, tokenFilePath string) (*GeminiWebClient, error) {
    // Build a minimal HTTP client (only for logging utilities). The core API
    // requests will go through geminiwebapi client which manages its own client.
    jar, _ := cookiejar.New(nil)
    httpClient := util.SetProxy(cfg, &http.Client{Jar: jar})

	// Guard substring length for clientID generation
	idPrefix := ts.Secure1PSID
	if len(idPrefix) > 8 {
		idPrefix = idPrefix[:8]
	}
    clientID := fmt.Sprintf("gemini-web-%s-%d", idPrefix, time.Now().UnixNano())
    client := &GeminiWebClient{
		ClientBase: ClientBase{
			RequestMutex:       &sync.Mutex{},
			httpClient:         httpClient,
			cfg:                cfg,
			tokenStorage:       ts,
			modelQuotaExceeded: make(map[string]*time.Time),
		},
        tokenFilePath: tokenFilePath,
        convStore:     make(map[string][]string),
    }

    // Load persisted conversation metadata for multi-turn continuity across restarts
    _ = client.loadConvStore()

    client.InitializeModelRegistry(clientID)
    // Register Gemini App specific model aliases to avoid pool round-robin
    client.registerGeminiWebModels()
    // Initialize Gemini Web API client
    client.gwc = gemweb.NewGeminiClient(ts.Secure1PSID, ts.Secure1PSIDTS, cfg.ProxyURL)
    if err := client.gwc.Init(300, false, 300, true, 540, false); err != nil {
        log.Warnf("Gemini Web init failed for %s: %v. Will retry in background.", client.GetEmail(), err)
        go client.backgroundInitRetry()
    } else {
        client.cookieRotationStarted = true // auto-refresh handled inside gwc
        // start persistence watcher for rotated cookies
        client.startCookiePersist()
    }

    return client, nil
}

// Init initializes the GeminiAppClient
func (c *GeminiWebClient) Init() error {
    // Initialize underlying web client (retry logic handled by caller)
    ts := c.tokenStorage.(*gemini.GeminiAppTokenStorage)
    c.gwc = gemweb.NewGeminiClient(ts.Secure1PSID, ts.Secure1PSIDTS, c.cfg.ProxyURL)
    if err := c.gwc.Init(300, false, 300, true, 540, false); err != nil {
        return err
    }
    // restart cookie persist loop after re-init
    c.startCookiePersist()
    return nil
}

func (c *GeminiWebClient) Type() string {
	return GEMINI
}

func (c *GeminiWebClient) Provider() string {
    return GEMINI
}

func (c *GeminiWebClient) CanProvideModel(modelName string) bool {
    // Only provide uniquely-named Gemini Web models to avoid pool round-robin
    return util.InArray([]string{"gemini-web-2.5-pro", "gemini-web-2.5-flash"}, modelName)
}

func (c *GeminiWebClient) GetEmail() string {
    base := filepath.Base(c.tokenFilePath)
    return strings.TrimSuffix(base, ".json")
}

func (c *GeminiWebClient) SendRawMessage(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
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
    // Write inline files to temp and use file paths for web API client
    uploadedFiles, upErr := c.materializeInlineFiles(files, mimes)
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

    log.Debugf("Use Gemini Web account %s for model %s", c.GetEmail(), modelName)
    // Perform generation via the web API (single request; session handles multi-turn)
    output, genErr := c.generateWithChat(ctx, modelName, prompt, meta, uploadedFiles...)
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

func (c *GeminiWebClient) SendRawMessageStream(ctx context.Context, modelName string, rawJSON []byte, alt string) (<-chan []byte, <-chan *interfaces.ErrorMessage) {
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

        // Build messages and upload inline files if any
        messages, files, mimes, err := c.parseMessagesAndFiles(rawJSON)
        if err != nil {
            errChan <- &interfaces.ErrorMessage{StatusCode: 400, Error: fmt.Errorf("bad request: %w", err)}
            return
        }
        uploadedFiles, upErr := c.materializeInlineFiles(files, mimes)
        if upErr != nil {
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

        log.Debugf("Use Gemini Web account %s for model %s", c.GetEmail(), modelName)
        // Single request; session metadata handles multi-turn
        output, genErr := c.generateWithChat(ctx, modelName, prompt, meta, uploadedFiles...)
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
            errChan <- &interfaces.ErrorMessage{StatusCode: status, Error: genErr}
            return
        }

        // Clear quota status on success
        delete(c.modelQuotaExceeded, modelName)
        c.ClearModelQuotaExceeded(modelName)

        // Build one final Gemini-shaped response and stream it as a single event
        gemBytes, errMsg := c.convertOutputToGemini(output, modelName)
        if errMsg != nil { errChan <- errMsg; return }
        c.AddAPIResponseData(ctx, gemBytes)
        if output != nil && len(output.Metadata) > 0 && len(output.Candidates) > 0 {
            c.storeConversation(modelName, cleaned, output.Candidates[0].Text, output.Metadata)
        }
        if translator.NeedConvert(handlerType, c.Type()) && handlerType != GEMINI {
            var param any
            lines := translator.Response(handlerType, c.Type(), ctx, modelName, originalRequestRawJSON, rawJSON, gemBytes, &param)
            for _, l := range lines { if l != "" { dataChan <- []byte(l) } }
            return
        }
        dataChan <- gemBytes
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
		"responseId":   fmt.Sprintf("gemini-web-%d", time.Now().UnixNano()),
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

func simulateGeminiStreaming(out chan<- []byte, modelName string, output *gemweb.ModelOutput) {
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
    if output.Candidates[0].Thoughts != nil {
        t := strings.TrimSpace(*output.Candidates[0].Thoughts)
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
            // If there are generated images, send them as inlineData chunks before STOP
            if imgs := output.Candidates[0].GeneratedImages; len(imgs) > 0 {
                for _, gi := range imgs {
                    if mime, data, err := fetchGeneratedImageData(gi); err == nil && data != "" {
                        out <- buildGeminiImageChunk(modelName, mime, data)
                    }
                }
            }
            out <- buildGeminiChunk(modelName, "", false, "STOP", true, true)
        } else {
            out <- buildGeminiChunk(modelName, textChunks[i], false, "", false, false)
        }
    }
}

// buildGeminiImageChunk builds a Gemini-like JSON chunk that carries a single inline image.
func buildGeminiImageChunk(modelName, mimeType, base64Data string) []byte {
    part := map[string]any{
        "inlineData": map[string]any{
            "mime_type": mimeType,
            "data":      base64Data,
        },
    }
    resp := map[string]any{
        "candidates": []any{
            map[string]any{
                "content": map[string]any{"parts": []any{part}, "role": "model"},
                "index":   0,
            },
        },
        "createTime":   time.Now().Format(time.RFC3339Nano),
        "responseId":   fmt.Sprintf("gemini-web-%d", time.Now().UnixNano()),
        "modelVersion": modelName,
    }
    b, _ := json.Marshal(resp)
    return b
}

// fetchGeneratedImageData downloads a generated image using the web client's cookies and returns (mime, base64Data).
func fetchGeneratedImageData(gi gemweb.GeneratedImage) (string, string, error) {
    // Save to temp file (allowed as an intermediate step), then read and remove.
    path, err := gi.Save("", "", true, false, true, false)
    if err != nil { return "", "", err }
    defer func() { _ = os.Remove(path) }()
    b, err := os.ReadFile(path)
    if err != nil { return "", "", err }
    // Detect MIME from content
    mime := http.DetectContentType(b)
    // Fallback if detection is generic
    if !strings.HasPrefix(mime, "image/") {
        // try by extension
        ext := strings.ToLower(filepath.Ext(path))
        switch ext {
        case ".png":
            mime = "image/png"
        case ".jpg", ".jpeg":
            mime = "image/jpeg"
        case ".webp":
            mime = "image/webp"
        case ".gif":
            mime = "image/gif"
        default:
            mime = "image/png"
        }
    }
    return mime, base64.StdEncoding.EncodeToString(b), nil
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
    output *gemweb.ModelOutput,
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
    if output.Candidates[0].Thoughts != nil {
        t := strings.TrimSpace(*output.Candidates[0].Thoughts)
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
            // emit inline image chunks if any
            if imgs := output.Candidates[0].GeneratedImages; len(imgs) > 0 {
                for _, gi := range imgs {
                    if mime, data, err := fetchGeneratedImageData(gi); err == nil && data != "" {
                        imgLines := translator.Response(handlerType, providerType, ctx, modelName, originalRequestRawJSON, requestRawJSON, buildGeminiImageChunk(modelName, mime, data), &param)
                        for _, l := range imgLines {
                            if l != "" { dataChan <- []byte(l) }
                        }
                    }
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

func (c *GeminiWebClient) SendRawTokenCount(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
    // No web endpoint for token counting; return a minimal Gemini-like usage structure
    return []byte(`{"totalTokens":0}`), nil
}

func (c *GeminiWebClient) SaveTokenToFile() error {
    ts := c.tokenStorage.(*gemini.GeminiAppTokenStorage)
    // Update storage from current web client cookies if available
    if c.gwc != nil && c.gwc.Cookies != nil {
        if v, ok := c.gwc.Cookies["__Secure-1PSID"]; ok {
            ts.Secure1PSID = v
        }
        if v, ok := c.gwc.Cookies["__Secure-1PSIDTS"]; ok {
            ts.Secure1PSIDTS = v
        }
    }
    log.Infof("Saving Gemini Web credentials to %s", c.tokenFilePath)
    return ts.SaveTokenToFile(c.tokenFilePath)
}

func (c *GeminiWebClient) IsModelQuotaExceeded(model string) bool {
	if t, ok := c.modelQuotaExceeded[model]; ok {
		return time.Since(*t) <= 30*time.Minute
	}
	return false
}

func (c *GeminiWebClient) GetUserAgent() string { return geminiAppUserAgent }

// GetRequestMutex returns nil to opt-out of handler-level per-client locking.
// This matches other clients in the codebase and prevents potential deadlocks
// if a long-lived stream holds the lock beyond expected scopes.
func (c *GeminiWebClient) GetRequestMutex() *sync.Mutex {
	return nil
}

func (c *GeminiWebClient) RefreshTokens(ctx context.Context) error {
    // Re-init underlying web client to refresh cookies/token
    return c.Init()
}

// registerGeminiAppModels registers uniquely named models for the Gemini App client
// to avoid being included in the general Gemini round-robin pool.
func (c *GeminiWebClient) registerGeminiWebModels() {
    models := []*registry.ModelInfo{
        {
            ID:          "gemini-web-2.5-flash",
            Object:      "model",
            Created:     time.Now().Unix(),
            OwnedBy:     "google",
            Type:        GEMINI,
            Name:        "gemini-web-2.5-flash",
            Version:     "2.5",
            Description: "Stable version of Gemini 2.5 Flash, our mid-size multimodal model that supports up to 1 million tokens, released in June of 2025.",
            InputTokenLimit: 1048576,
            OutputTokenLimit: 65536,
            SupportedParameters: []string{"tools", "vision", "thinking"},
        },
        {
            ID:          "gemini-web-2.5-pro",
            Object:      "model",
            Created:     time.Now().Unix(),
            OwnedBy:     "google",
            Type:        GEMINI,
            Name:        "gemini-web-2.5-pro",
            Version:     "2.5",
            Description: "Stable release (June 17th, 2025) of Gemini 2.5 Pro",
            InputTokenLimit: 1048576,
            OutputTokenLimit: 65536,
            SupportedParameters: []string{"tools", "vision", "thinking"},
        },
    }
    c.RegisterModels(GEMINI, models)
}

// mapAliasToUnderlying converts our public alias model names to geminiwebapi model names.
func mapAliasToUnderlying(name string) string {
    switch strings.ToLower(name) {
    case "gemini-web-2.5-pro":
        return "gemini-2.5-pro"
    case "gemini-web-2.5-flash":
        return "gemini-2.5-flash"
    default:
        // If user passes original names, pass through for compatibility
        return name
    }
}

// ---------- Persistence of conversation metadata ----------
func (c *GeminiWebClient) convStorePath() string {
    // Store conversations under <program-working-dir>/conv/
    wd, err := os.Getwd()
    if err != nil || wd == "" {
        wd = "."
    }
    convDir := filepath.Join(wd, "conv")
    base := strings.TrimSuffix(filepath.Base(c.tokenFilePath), filepath.Ext(c.tokenFilePath))
    return filepath.Join(convDir, base+".conv.json")
}

func (c *GeminiWebClient) loadConvStore() error {
    path := c.convStorePath()
    b, err := os.ReadFile(path)
    if err != nil {
        return nil // ignore missing file
    }
    var tmp map[string][]string
    if err := json.Unmarshal(b, &tmp); err != nil {
        return err
    }
    c.convMutex.Lock()
    c.convStore = tmp
    c.convMutex.Unlock()
    return nil
}

func (c *GeminiWebClient) saveConvStore() error {
    path := c.convStorePath()
    c.convMutex.RLock()
    data, err := json.MarshalIndent(c.convStore, "", "  ")
    c.convMutex.RUnlock()
    if err != nil { return err }
    tmp := path + ".tmp"
    // Ensure target directory exists
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { return err }
    if err := os.WriteFile(tmp, data, 0o644); err != nil { return err }
    return os.Rename(tmp, path)
}


func (c *GeminiWebClient) backgroundInitRetry() {
    backoffs := []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second, 1 * time.Minute, 2 * time.Minute, 5 * time.Minute}
    i := 0
    for {
        if err := c.Init(); err == nil {
            log.Infof("Gemini Web token recovered for %s", c.GetEmail())
            if !c.cookieRotationStarted {
                c.cookieRotationStarted = true
                // Auto refresh is handled inside geminiwebapi client
            }
            // ensure persistence loop is running
            c.startCookiePersist()
            return
        }
        d := backoffs[i]
        if i < len(backoffs)-1 {
            i++
        }
        time.Sleep(d)
    }
}

// startCookiePersist starts a lightweight loop that detects cookie rotation
// from the underlying web client and persists refreshes to the token file.
func (c *GeminiWebClient) startCookiePersist() {
    if c.gwc == nil {
        return
    }
    // cancel previous loop if running
    if c.cookiePersistCancel != nil {
        c.cookiePersistCancel()
        c.cookiePersistCancel = nil
    }
    ctx, cancel := context.WithCancel(context.Background())
    c.cookiePersistCancel = cancel

    go func() {
        ticker := time.NewTicker(60 * time.Second)
        defer ticker.Stop()
        last := ""
        if v, ok := c.gwc.Cookies["__Secure-1PSIDTS"]; ok {
            last = v
        }
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                cur := ""
                if c.gwc != nil && c.gwc.Cookies != nil {
                    if v, ok := c.gwc.Cookies["__Secure-1PSIDTS"]; ok {
                        cur = v
                    }
                }
                if cur != "" && cur != last {
                    if err := c.SaveTokenToFile(); err != nil {
                        log.Errorf("Failed to persist rotated cookies for %s: %v", c.GetEmail(), err)
                    } else {
                        log.Debugf("Persisted rotated cookies for %s", c.GetEmail())
                        last = cur
                    }
                }
            }
        }
    }()
}

type roleText struct {
    Role string
    Text string
}

func (c *GeminiWebClient) parseMessagesAndFiles(rawJSON []byte) ([]roleText, [][]byte, []string, error) {
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

func (c *GeminiWebClient) conversationKey(modelName string, msgs []roleText) string {
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

func (c *GeminiWebClient) findReusableMetadata(modelName string, msgs []roleText) ([]string, int) {
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

func (c *GeminiWebClient) storeConversation(modelName string, msgs []roleText, assistantReply string, metadata []string) {
    all := make([]roleText, 0, len(msgs)+1)
    all = append(all, msgs...)
    all = append(all, roleText{Role: "assistant", Text: removeThinkTags(assistantReply)})
    key := c.conversationKey(modelName, all)
    c.convMutex.Lock()
    c.convStore[key] = metadata
    c.convMutex.Unlock()
    _ = c.saveConvStore()
}

// generateWithChat sends a single request using a chat session with optional existing metadata.
func (c *GeminiWebClient) generateWithChat(ctx context.Context, modelName, prompt string, metadata []string, files ...string) (*gemweb.ModelOutput, error) {
    underlying := mapAliasToUnderlying(modelName)
    model, err := gemweb.ModelFromName(underlying)
    if err != nil { return nil, err }
    var chat *gemweb.ChatSession
    if len(metadata) > 0 { chat = c.gwc.StartChat(model, nil, metadata) } else { chat = c.gwc.StartChat(model, nil, nil) }
    out, err := c.generateContent(ctx, model, prompt, chat, files...)
    if err != nil { return nil, err }
    return out, nil
}

// materializeInlineFiles writes inline file bytes to temporary files and returns their paths.
func (c *GeminiWebClient) materializeInlineFiles(files [][]byte, mimes []string) ([]string, *interfaces.ErrorMessage) {
    if len(files) == 0 { return nil, nil }
    paths := make([]string, 0, len(files))
    for i, data := range files {
        ext := mimeToExt(mimes, i)
        f, err := os.CreateTemp("", "gemini-upload-*"+ext)
        if err != nil {
            return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to create temp file: %w", err)}
        }
        if _, err = f.Write(data); err != nil {
            _ = f.Close(); _ = os.Remove(f.Name())
            return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to write temp file: %w", err)}
        }
        if err = f.Close(); err != nil {
            _ = os.Remove(f.Name())
            return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to close temp file: %w", err)}
        }
        paths = append(paths, f.Name())
    }
    return paths, nil
}

// generateContent calls the Gemini web client to generate output for a prompt, with optional files.
func (c *GeminiWebClient) generateContent(ctx context.Context, model gemweb.Model, prompt string, chat *gemweb.ChatSession, files ...string) (*gemweb.ModelOutput, error) {
    if c.gwc == nil {
        if err := c.Init(); err != nil { return nil, err }
    }
    out, err := c.gwc.GenerateContent(prompt, files, model, nil, chat)
    if err != nil {
        // Map known errors to our typed errors for status mapping
        switch err.(type) {
        case *gemweb.UsageLimitExceeded:
            return nil, errGeminiUsageLimitExceeded
        case *gemweb.ModelInvalid:
            return nil, errGeminiModelInvalid
        case *gemweb.TemporarilyBlocked:
            return nil, errGeminiTemporarilyBlocked
        case *gemweb.TimeoutError:
            return nil, fmt.Errorf("timeout: %w", err)
        }
        return nil, err
    }
    return &out, nil
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
func (c *GeminiWebClient) convertOutputToGemini(output *gemweb.ModelOutput, modelName string) ([]byte, *interfaces.ErrorMessage) {
    if output == nil || len(output.Candidates) == 0 {
        return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("empty output")}
    }

    parts := make([]map[string]any, 0, 2)
    // Include reasoning/thoughts as a dedicated part if present
    if output.Candidates[0].Thoughts != nil {
        if t := strings.TrimSpace(*output.Candidates[0].Thoughts); t != "" {
            parts = append(parts, map[string]any{"text": t, "thought": true})
        }
    }
    // Main assistant content
    parts = append(parts, map[string]any{"text": output.Candidates[0].Text})

    // Append generated images inline (download and embed as base64)
    if imgs := output.Candidates[0].GeneratedImages; len(imgs) > 0 {
        for _, gi := range imgs {
            if mime, data, err := fetchGeneratedImageData(gi); err == nil && data != "" {
                parts = append(parts, map[string]any{
                    "inlineData": map[string]any{
                        "mime_type": mime,
                        "data":      data,
                    },
                })
            }
        }
    }

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
		"responseId":   fmt.Sprintf("gemini-web-%d", time.Now().UnixNano()),
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
