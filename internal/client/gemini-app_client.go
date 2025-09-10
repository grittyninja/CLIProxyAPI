package client

import (
    "bytes"
    "context"
    "errors"
    "encoding/base64"
	"encoding/json"
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
    "github.com/luispater/CLIProxyAPI/internal/util"
    "github.com/luispater/CLIProxyAPI/internal/translator/translator"
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

	log.Debugf("Cookie Jar Content: %+v", jar.Cookies(cookieURL))

    // Build HTTP client with shared proxy handling (supports socks5/http/https)
    httpClient := util.SetProxy(cfg, &http.Client{Jar: jar})

	clientID := fmt.Sprintf("gemini-app-%s-%d", ts.Secure1PSID[:8], time.Now().UnixNano())
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
	}

	client.InitializeModelRegistry(clientID)
	client.RegisterModels(GEMINI, registry.GetGeminiModels())

	// Fetch initial access token
	if err := client.Init(); err != nil {
		return nil, fmt.Errorf("failed to initialize gemini-app client: %w", err)
	}

	go client.startCookieRotation()

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
    // Normalize request into Gemini-style JSON if coming from OpenAI handler
    if handler, ok := ctx.Value("handler").(interfaces.APIHandler); ok {
        rawJSON = translator.Request(handler.HandlerType(), c.Type(), modelName, rawJSON, false)
    }
    // Log upstream API request body for request logger
    if c.cfg.RequestLog {
        if ginContext, ok := ctx.Value("gin").(*gin.Context); ok {
            ginContext.Set("API_REQUEST", rawJSON)
        }
    }
    prompt, files, mimes, err := c.extractRequestData(rawJSON)
    if err != nil {
        return nil, &interfaces.ErrorMessage{StatusCode: 400, Error: fmt.Errorf("bad request: %w", err)}
    }

	var uploadedFiles []*UploadedFile
    for i, fileData := range files {
        ext := mimeToExt(mimes, i)
        tmpfile, err := os.CreateTemp("", "gemini-upload-*"+ext)
		if err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to create temp file: %w", err)}
		}
		defer os.Remove(tmpfile.Name())

		if _, err := tmpfile.Write(fileData); err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to write to temp file: %w", err)}
		}
		if err := tmpfile.Close(); err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to close temp file: %w", err)}
		}
		uploadedFile, err := c.uploadFile(tmpfile.Name())
		if err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to upload file: %w", err)}
		}
		uploadedFiles = append(uploadedFiles, uploadedFile)
	}

    output, err := c.generateContent(ctx, modelName, prompt, "", uploadedFiles...)
    if err != nil {
        log.Errorf("failed to generate content: %v", err)
        status := 500
        switch {
        case errors.Is(err, errGeminiUsageLimitExceeded), errors.Is(err, errGeminiTemporarilyBlocked):
            status = 429
        case errors.Is(err, errGeminiModelInconsistent), errors.Is(err, errGeminiModelInvalid):
            status = 400
        }
        if status == 429 {
            now := time.Now()
            c.modelQuotaExceeded[modelName] = &now
            c.SetModelQuotaExceeded(modelName)
        }
        return nil, &interfaces.ErrorMessage{StatusCode: status, Error: err}
    }

    // Clear quota status on success
    delete(c.modelQuotaExceeded, modelName)
    c.ClearModelQuotaExceeded(modelName)
    return c.convertOutputToV1Beta(output, modelName)
}

func (c *GeminiAppClient) SendRawMessageStream(ctx context.Context, modelName string, rawJSON []byte, alt string) (<-chan []byte, <-chan *interfaces.ErrorMessage) {
    dataChan := make(chan []byte)
    errChan := make(chan *interfaces.ErrorMessage)

    go func() {
        defer close(dataChan)
        defer close(errChan)

        // Normalize request into Gemini-style JSON if coming from OpenAI handler
        if handler, ok := ctx.Value("handler").(interfaces.APIHandler); ok {
            rawJSON = translator.Request(handler.HandlerType(), c.Type(), modelName, rawJSON, true)
        }

        // Log upstream API request body for request logger
        if c.cfg.RequestLog {
            if ginContext, ok := ctx.Value("gin").(*gin.Context); ok {
                ginContext.Set("API_REQUEST", rawJSON)
            }
        }

        // Build prompt/files and upload inline files if any
        prompt, files, mimes, err := c.extractRequestData(rawJSON)
        if err != nil {
            errChan <- &interfaces.ErrorMessage{StatusCode: 400, Error: fmt.Errorf("bad request: %w", err)}
            return
        }

        var uploadedFiles []*UploadedFile
        for i, fileData := range files {
            ext := mimeToExt(mimes, i)
            tmpfile, e := os.CreateTemp("", "gemini-upload-*"+ext)
            if e != nil {
                errChan <- &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to create temp file: %w", e)}
                return
            }
            _, _ = tmpfile.Write(fileData)
            _ = tmpfile.Close()
            uploadedFile, e := c.uploadFile(tmpfile.Name())
            os.Remove(tmpfile.Name())
            if e != nil {
                errChan <- &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to upload file: %w", e)}
                return
            }
            uploadedFiles = append(uploadedFiles, uploadedFile)
        }

        // Call upstream Gemini App
        output, genErr := c.generateContent(ctx, modelName, prompt, "", uploadedFiles...)
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

        // Build minimal OpenAI chat.completion.chunk stream (JSON only)
        // 1) role chunk
        created := time.Now().Unix()
        id := fmt.Sprintf("%d-%s", created, "gemini-app")
        roleChunk := fmt.Sprintf(`{"id":"%s","object":"chat.completion.chunk","created":%d,"model":"%s","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null,"native_finish_reason":null}]}`,
            id, created, modelName)
        dataChan <- []byte(roleChunk)

        // 2) reasoning chunks (if any)
        if output != nil && len(output.Candidates) > 0 && strings.TrimSpace(output.Candidates[0].Thoughts) != "" {
            segments := splitReasoningSegments(output.Candidates[0].Thoughts)
            for _, seg := range segments {
                rcChunk := fmt.Sprintf(`{"id":"%s","object":"chat.completion.chunk","created":%d,"model":"%s","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":%s,"tool_calls":null},"finish_reason":null,"native_finish_reason":null}],"usage":{"prompt_tokens":0}}`,
                    id, created, modelName, mustJSONMarshalString(seg))
                dataChan <- []byte(rcChunk)
            }
        }

        // 3) final content chunk
        content := ""
        if output != nil && len(output.Candidates) > 0 {
            content = output.Candidates[0].Text
        }
        finalChunk := fmt.Sprintf(`{"id":"%s","object":"chat.completion.chunk","created":%d,"model":"%s","choices":[{"index":0,"delta":{"role":"assistant","content":%s},"finish_reason":"STOP","native_finish_reason":"STOP"}],"usage":{"completion_tokens":0,"total_tokens":0,"prompt_tokens":0}}`,
            id, created, modelName, mustJSONMarshalString(content))
        dataChan <- []byte(finalChunk)

        // 4) Stream complete; handler will emit final [DONE]
    }()

    return dataChan, errChan
}

// mustJSONMarshalString safely marshals a string into JSON string literal
func mustJSONMarshalString(s string) string {
    b, err := json.Marshal(s)
    if err != nil {
        esc := strings.ReplaceAll(s, "\"", "\\\"")
        esc = strings.ReplaceAll(esc, "\n", "\\n")
        return fmt.Sprintf("\"%s\"", esc)
    }
    return string(b)
}

func splitReasoningSegments(s string) []string {
    s = strings.TrimSpace(s)
    if s == "" {
        return nil
    }
    parts := strings.Split(s, "\n\n")
    out := make([]string, 0, len(parts))
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p != "" {
            out = append(out, p)
        }
    }
    return out
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
	return []byte(`{"tokenCount": 0}`), nil
}

func (c *GeminiAppClient) SaveTokenToFile() error {
	ts := c.tokenStorage.(*gemini.GeminiAppTokenStorage)
	log.Infof("Saving Gemini App credentials to %s", c.tokenFilePath)
	return ts.SaveTokenToFile(c.tokenFilePath)
}

func (c *GeminiAppClient) IsModelQuotaExceeded(model string) bool {
	if lastExceededTime, hasKey := c.modelQuotaExceeded[model]; hasKey {
		duration := time.Now().Sub(*lastExceededTime)
		if duration > 30*time.Minute {
			return false
		}
		return true
	}
	return false
}

func (c *GeminiAppClient) GetUserAgent() string {
	return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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

	var modelHeader string
	switch modelName {
	case "gemini-2.5-flash", "gemini-2.5-flash-lite":
		modelHeader = geminiAppModelFlash
	case "gemini-2.5-pro":
		modelHeader = geminiAppModelPro
	default:
		modelHeader = geminiAppModelPro
	}
	req.Header.Set("x-goog-ext-525001261-jspb", modelHeader)
}

func (c *GeminiAppClient) extractRequestData(rawJSON []byte) (string, [][]byte, []string, error) {
    var prompt strings.Builder
    var files [][]byte
    var mimes []string

	contents := gjson.GetBytes(rawJSON, "contents")
	if contents.Exists() {
		contents.ForEach(func(_, content gjson.Result) bool {
			content.Get("parts").ForEach(func(_, part gjson.Result) bool {
				if text := part.Get("text"); text.Exists() {
					prompt.WriteString(text.String())
					prompt.WriteString("\n")
				}
                if inlineData := part.Get("inlineData"); inlineData.Exists() {
                    data := inlineData.Get("data").String()
                    b, err := base64.StdEncoding.DecodeString(data)
                    if err == nil {
                        files = append(files, b)
                        m := inlineData.Get("mime_type").String()
                        mimes = append(mimes, m)
                    }
                }
				return true
			})
			return true
		})
	}

    return strings.TrimSpace(prompt.String()), files, mimes, nil
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

    // Use a local client to preserve POST + body across redirects
    localClient := &http.Client{
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
        Transport: c.httpClient.Transport,
        Jar:       c.httpClient.Jar,
        Timeout:   c.httpClient.Timeout,
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

func (c *GeminiAppClient) generateContent(ctx context.Context, modelName, prompt, gemID string, files ...*UploadedFile) (*ModelOutput, error) {
	// This is a simplified version of the python client's request construction
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

	req, err := http.NewRequestWithContext(ctx, "POST", geminiAppGenerate, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	c.setHeaders(req, modelName)

	log.Debugf("Making request with Gemini App client: %s", c.GetEmail())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        // Log upstream API response even on error
        c.AddAPIResponseData(ctx, body)
        return nil, fmt.Errorf("API request failed with status code %d: %s", resp.StatusCode, string(body))
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    // Log upstream API response body for request logger
    c.AddAPIResponseData(ctx, body)

	lines := strings.Split(string(body), "\n")
	var responseData [][]interface{}
	for _, line := range lines {
		if strings.HasPrefix(line, "[[") {
			if err := json.Unmarshal([]byte(line), &responseData); err == nil {
				break
			}
		}
	}
	if responseData == nil {
		return nil, fmt.Errorf("failed to find valid JSON data in response: %s", string(body))
	}

    var textBuilder strings.Builder
    var thoughtsBuilder strings.Builder
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
        }, nil
}

// Helper functions for minimal nested slice traversal (error code extraction)
func safeGet(data []interface{}, index int) (interface{}, error) {
    if len(data) <= index {
        return nil, fmt.Errorf("index %d out of bounds for slice of length %d", index, len(data))
    }
    return data[index], nil
}

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

func (c *GeminiAppClient) convertOutputToV1Beta(output *ModelOutput, modelName string) ([]byte, *interfaces.ErrorMessage) {
    // Return OpenAI Chat Completions non-stream format for better compatibility
    id := fmt.Sprintf("%d-%s", time.Now().UnixNano(), "gemini-app")
    created := time.Now().Unix()
    content := jsonEscape(output.Candidates[0].Text)
    thoughts := output.Candidates[0].Thoughts
    reasoning := "null"
    if strings.TrimSpace(thoughts) != "" {
        reasoning = fmt.Sprintf("\"%s\"", jsonEscape(thoughts))
    }
    resp := fmt.Sprintf(`{"id":"%s","object":"chat.completion","created":%d,"model":"%s","choices":[{"index":0,"message":{"role":"assistant","content":"%s","reasoning_content":%s},"finish_reason":"STOP","native_finish_reason":"STOP"}],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`,
        id, created, modelName, content, reasoning)
    return []byte(resp), nil
}

func jsonEscape(i string) string {
	b, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	s := string(b)
	return s[1 : len(s)-1]
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
