package client

import (
	"bytes"
	"context"
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

	"github.com/luispater/CLIProxyAPI/internal/config"
	. "github.com/luispater/CLIProxyAPI/internal/constant"
	"github.com/luispater/CLIProxyAPI/internal/interfaces"
	"github.com/luispater/CLIProxyAPI/internal/registry"
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
)

type GeminiAppClient struct {
	ClientBase
	secure1psid   string
	secure1psidts string
	accessToken   string
	cookies       []*http.Cookie
}

func NewGeminiAppClient(cfg *config.Config, secure1psid, secure1psidts string) (*GeminiAppClient, error) {
	jar, _ := cookiejar.New(nil)
	cookieURL, _ := url.Parse(geminiAppBaseURL)
	cookies := []*http.Cookie{
		{Name: "__Secure-1PSID", Value: secure1psid, Domain: ".google.com"},
	}
	if secure1psidts != "" {
		cookies = append(cookies, &http.Cookie{Name: "__Secure-1PSIDTS", Value: secure1psidts, Domain: ".google.com"})
	}
	jar.SetCookies(cookieURL, cookies)

	log.Debugf("Cookie Jar Content: %+v", jar.Cookies(cookieURL))

	transport := &http.Transport{}
	if cfg.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	httpClient := &http.Client{
		Jar:       jar,
		Transport: transport,
	}

	clientID := fmt.Sprintf("gemini-app-%s-%d", secure1psid[:8], time.Now().UnixNano())
	client := &GeminiAppClient{
		ClientBase: ClientBase{
			RequestMutex:       &sync.Mutex{},
			httpClient:         httpClient,
			cfg:                cfg,
			modelQuotaExceeded: make(map[string]*time.Time),
		},
		secure1psid:   secure1psid,
		secure1psidts: secure1psidts,
		cookies:       cookies,
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
	return fmt.Sprintf("cookie-%s", c.secure1psid[:8])
}

func (c *GeminiAppClient) SendRawMessage(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
	prompt, files, err := c.extractRequestData(rawJSON)
	if err != nil {
		return nil, &interfaces.ErrorMessage{StatusCode: 400, Error: fmt.Errorf("bad request: %w", err)}
	}

	var uploadedFiles []*UploadedFile
	for _, fileData := range files {
		tmpfile, err := os.CreateTemp("", "gemini-upload-*.jpg")
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
		return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to generate content: %w", err)}
	}

	return c.convertOutputToV1Beta(output, modelName)
}

func (c *GeminiAppClient) SendRawMessageStream(ctx context.Context, modelName string, rawJSON []byte, alt string) (<-chan []byte, <-chan *interfaces.ErrorMessage) {
	dataChan := make(chan []byte)
	errChan := make(chan *interfaces.ErrorMessage)

	go func() {
		defer close(dataChan)
		defer close(errChan)

		resp, err := c.SendRawMessage(ctx, modelName, rawJSON, alt)
		if err != nil {
			errChan <- err
			return
		}

		dataChan <- resp
		dataChan <- []byte("[DONE]")
	}()

	return dataChan, errChan
}

func (c *GeminiAppClient) SendRawTokenCount(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
	return []byte(`{"tokenCount": 0}`), nil
}

func (c *GeminiAppClient) SaveTokenToFile() error {
	return nil
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
	return nil
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

func (c *GeminiAppClient) extractRequestData(rawJSON []byte) (string, [][]byte, error) {
	var prompt strings.Builder
	var files [][]byte

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
					}
				}
				return true
			})
			return true
		})
	}

	return strings.TrimSpace(prompt.String()), files, nil
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
	resp, err := c.httpClient.Do(req)
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

	log.Debugf("Request cookies: %+v", req.Cookies())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status code %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

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
					textSlice, _ := candList[1].([]interface{})
					if len(textSlice) > 0 {
						text, _ := textSlice[0].(string)
						textBuilder.WriteString(text)
					}
				}
			}
		}
	}

	return &ModelOutput{
		Candidates: []Candidate{
			{Text: textBuilder.String()},
		},
	}, nil
}

func (c *GeminiAppClient) convertOutputToV1Beta(output *ModelOutput, modelName string) ([]byte, *interfaces.ErrorMessage) {
	// A simplified conversion for now
	resp := fmt.Sprintf(`{
		"candidates": [
			{
				"content": {
					"parts": [
						{
							"text": "%s"
						}
					],
					"role": "model"
				},
				"finishReason": "STOP"
			}
		],
		"usageMetadata": {
			"promptTokenCount": 0,
			"candidatesTokenCount": 0,
			"totalTokenCount": 0
		}
	}`, jsonEscape(output.Candidates[0].Text))

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
	Text string
}

type UploadedFile struct {
	Identifier []string
	FileName   string
}
