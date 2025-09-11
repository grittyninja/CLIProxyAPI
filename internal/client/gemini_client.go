// Package client defines the interface and base structure for AI API clients.
// It provides a common interface that all supported AI service clients must implement,
// including methods for sending messages, handling streams, and managing authentication.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/luispater/CLIProxyAPI/internal/config"
	. "github.com/luispater/CLIProxyAPI/internal/constant"
	"github.com/luispater/CLIProxyAPI/internal/interfaces"
	"github.com/luispater/CLIProxyAPI/internal/registry"
	"github.com/luispater/CLIProxyAPI/internal/translator/translator"
	"github.com/luispater/CLIProxyAPI/internal/util"
	log "github.com/sirupsen/logrus"
)

const (
	glEndPoint   = "https://generativelanguage.googleapis.com"
	glAPIVersion = "v1beta"
)

// GeminiClient is the main client for interacting with the CLI API.
type GeminiClient struct {
	ClientBase
	glAPIKey string
}

// NewGeminiClient creates a new CLI API client.
//
// Parameters:
//   - httpClient: The HTTP client to use for requests.
//   - cfg: The application configuration.
//   - glAPIKey: The Google Cloud API key.
//
// Returns:
//   - *GeminiClient: A new Gemini client instance.
func NewGeminiClient(httpClient *http.Client, cfg *config.Config, glAPIKey string) *GeminiClient {
	// Generate unique client ID
	clientID := fmt.Sprintf("gemini-apikey-%s-%d", glAPIKey, time.Now().UnixNano())

	client := &GeminiClient{
		ClientBase: ClientBase{
			RequestMutex:       &sync.Mutex{},
			httpClient:         httpClient,
			cfg:                cfg,
			modelQuotaExceeded: make(map[string]*time.Time),
		},
		glAPIKey: glAPIKey,
	}

	// Initialize model registry and register Gemini models
	client.InitializeModelRegistry(clientID)
	client.RegisterModels("gemini", registry.GetGeminiModels())

	return client
}

// Type returns the client type
func (c *GeminiClient) Type() string {
	return GEMINI
}

// Provider returns the provider name for this client.
func (c *GeminiClient) Provider() string {
	return GEMINI
}

// CanProvideModel checks if this client can provide the specified model.
//
// Parameters:
//   - modelName: The name of the model to check.
//
// Returns:
//   - bool: True if the model is supported, false otherwise.
func (c *GeminiClient) CanProvideModel(modelName string) bool {
	models := []string{
		"gemini-2.5-pro",
		"gemini-2.5-flash",
		"gemini-2.5-flash-lite",
	}
	return util.InArray(models, modelName)
}

// GetEmail returns the email address associated with the client's token storage.
func (c *GeminiClient) GetEmail() string {
	return c.glAPIKey
}

// APIRequest handles making requests to the CLI API endpoints.
//
// Parameters:
//   - ctx: The context for the request.
//   - modelName: The name of the model to use.
//   - endpoint: The API endpoint to call.
//   - body: The request body.
//   - alt: An alternative response format parameter.
//   - stream: A boolean indicating if the request is for a streaming response.
//
// Returns:
//   - io.ReadCloser: The response body reader.
//   - *interfaces.ErrorMessage: An error message if the request fails.
func (c *GeminiClient) APIRequest(ctx context.Context, modelName, endpoint string, body interface{}, alt string, stream bool) (io.ReadCloser, *interfaces.ErrorMessage) {
	var jsonBody []byte
	var err error
	if byteBody, ok := body.([]byte); ok {
		jsonBody = byteBody
	} else {
		jsonBody, err = json.Marshal(body)
		if err != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to marshal request body: %w", err)}
		}
	}

	var url string
	if endpoint == "countTokens" {
		url = fmt.Sprintf("%s/%s/models/%s:%s", glEndPoint, glAPIVersion, modelName, endpoint)
	} else {
		url = fmt.Sprintf("%s/%s/models/%s:%s", glEndPoint, glAPIVersion, modelName, endpoint)
		if alt == "" && stream {
			url = url + "?alt=sse"
		} else {
			if alt != "" {
				url = url + fmt.Sprintf("?$alt=%s", alt)
			}
		}
	}

	// log.Debug(string(jsonBody))
	// log.Debug(url)
	reqBody := bytes.NewBuffer(jsonBody)

	req, err := http.NewRequestWithContext(ctx, "POST", url, reqBody)
	if err != nil {
		return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to create request: %v", err)}
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", c.glAPIKey)

	if c.cfg.RequestLog {
		if ginContext, ok := ctx.Value("gin").(*gin.Context); ok {
			ginContext.Set("API_REQUEST", jsonBody)
		}
	}

	log.Debugf("Use Gemini API key %s for model %s", util.HideAPIKey(c.GetEmail()), modelName)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: fmt.Errorf("failed to execute request: %v", err)}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer func() {
			if err = resp.Body.Close(); err != nil {
				log.Printf("warn: failed to close response body: %v", err)
			}
		}()
		bodyBytes, _ := io.ReadAll(resp.Body)
		// log.Debug(string(jsonBody))
		return nil, &interfaces.ErrorMessage{StatusCode: resp.StatusCode, Error: fmt.Errorf("%s", string(bodyBytes))}
	}

	return resp.Body, nil
}

// SendRawTokenCount handles a token count.
//
// Parameters:
//   - ctx: The context for the request.
//   - modelName: The name of the model to use.
//   - rawJSON: The raw JSON request body.
//   - alt: An alternative response format parameter.
//
// Returns:
//   - []byte: The response body.
//   - *interfaces.ErrorMessage: An error message if the request fails.
func (c *GeminiClient) SendRawTokenCount(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
	originalRequestRawJSON := bytes.Clone(rawJSON)
	for {
		if c.IsModelQuotaExceeded(modelName) {
			return nil, &interfaces.ErrorMessage{
				StatusCode: 429,
				Error:      fmt.Errorf(`{"error":{"code":429,"message":"All the models of '%s' are quota exceeded","status":"RESOURCE_EXHAUSTED"}}`, modelName),
			}
		}

		handler := ctx.Value("handler").(interfaces.APIHandler)
		handlerType := handler.HandlerType()
		rawJSON = translator.Request(handlerType, c.Type(), modelName, rawJSON, false)

		respBody, err := c.APIRequest(ctx, modelName, "countTokens", rawJSON, alt, false)
		if err != nil {
			if err.StatusCode == 429 {
				now := time.Now()
				c.modelQuotaExceeded[modelName] = &now
				// Update model registry quota status
				c.SetModelQuotaExceeded(modelName)
			}
			return nil, err
		}
		delete(c.modelQuotaExceeded, modelName)
		// Clear quota status in model registry
		c.ClearModelQuotaExceeded(modelName)
		bodyBytes, errReadAll := io.ReadAll(respBody)
		if errReadAll != nil {
			return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: errReadAll}
		}

		c.AddAPIResponseData(ctx, bodyBytes)
		var param any
		bodyBytes = []byte(translator.ResponseNonStream(handlerType, c.Type(), ctx, modelName, originalRequestRawJSON, rawJSON, bodyBytes, &param))

		return bodyBytes, nil
	}
}

// SendRawMessage handles a single conversational turn, including tool calls.
//
// Parameters:
//   - ctx: The context for the request.
//   - modelName: The name of the model to use.
//   - rawJSON: The raw JSON request body.
//   - alt: An alternative response format parameter.
//
// Returns:
//   - []byte: The response body.
//   - *interfaces.ErrorMessage: An error message if the request fails.
func (c *GeminiClient) SendRawMessage(ctx context.Context, modelName string, rawJSON []byte, alt string) ([]byte, *interfaces.ErrorMessage) {
	originalRequestRawJSON := bytes.Clone(rawJSON)

	handler := ctx.Value("handler").(interfaces.APIHandler)
	handlerType := handler.HandlerType()
	rawJSON = translator.Request(handlerType, c.Type(), modelName, rawJSON, false)

	if c.IsModelQuotaExceeded(modelName) {
		return nil, &interfaces.ErrorMessage{
			StatusCode: 429,
			Error:      fmt.Errorf(`{"error":{"code":429,"message":"All the models of '%s' are quota exceeded","status":"RESOURCE_EXHAUSTED"}}`, modelName),
		}
	}

	respBody, err := c.APIRequest(ctx, modelName, "generateContent", rawJSON, alt, false)
	if err != nil {
		if err.StatusCode == 429 {
			now := time.Now()
			c.modelQuotaExceeded[modelName] = &now
			// Update model registry quota status
			c.SetModelQuotaExceeded(modelName)
		}
		return nil, err
	}
	delete(c.modelQuotaExceeded, modelName)
	// Clear quota status in model registry
	c.ClearModelQuotaExceeded(modelName)
	bodyBytes, errReadAll := io.ReadAll(respBody)
	if errReadAll != nil {
		return nil, &interfaces.ErrorMessage{StatusCode: 500, Error: errReadAll}
	}

	_ = respBody.Close()
	c.AddAPIResponseData(ctx, bodyBytes)
	// log.Debugf("Gemini response: %s", string(bodyBytes))

	var param any
	output := []byte(translator.ResponseNonStream(handlerType, c.Type(), ctx, modelName, originalRequestRawJSON, rawJSON, bodyBytes, &param))

	return output, nil
}

// SendRawMessageStream handles a single conversational turn, including tool calls.
//
// Parameters:
//   - ctx: The context for the request.
//   - modelName: The name of the model to use.
//   - rawJSON: The raw JSON request body.
//   - alt: An alternative response format parameter.
//
// Returns:
//   - <-chan []byte: A channel for receiving response data chunks.
//   - <-chan *interfaces.ErrorMessage: A channel for receiving error messages.
func (c *GeminiClient) SendRawMessageStream(ctx context.Context, modelName string, rawJSON []byte, alt string) (<-chan []byte, <-chan *interfaces.ErrorMessage) {
	originalRequestRawJSON := bytes.Clone(rawJSON)

	handler := ctx.Value("handler").(interfaces.APIHandler)
	handlerType := handler.HandlerType()
	rawJSON = translator.Request(handlerType, c.Type(), modelName, rawJSON, true)

	dataTag := []byte("data: ")
	errChan := make(chan *interfaces.ErrorMessage)
	dataChan := make(chan []byte)
	// log.Debugf(string(rawJSON))
	// return dataChan, errChan
	go func() {
		defer close(errChan)
		defer close(dataChan)

		var stream io.ReadCloser
		if c.IsModelQuotaExceeded(modelName) {
			errChan <- &interfaces.ErrorMessage{
				StatusCode: 429,
				Error:      fmt.Errorf(`{"error":{"code":429,"message":"All the models of '%s' are quota exceeded","status":"RESOURCE_EXHAUSTED"}}`, modelName),
			}
			return
		}
		var err *interfaces.ErrorMessage
		stream, err = c.APIRequest(ctx, modelName, "streamGenerateContent", rawJSON, alt, true)
		if err != nil {
			if err.StatusCode == 429 {
				now := time.Now()
				c.modelQuotaExceeded[modelName] = &now
				// Update model registry quota status
				c.SetModelQuotaExceeded(modelName)
			}
			errChan <- err
			return
		}
		delete(c.modelQuotaExceeded, modelName)
		// Clear quota status in model registry
		c.ClearModelQuotaExceeded(modelName)
		defer func() {
			_ = stream.Close()
		}()

		newCtx := context.WithValue(ctx, "alt", alt)
		var param any
		if alt == "" {
			scanner := bufio.NewScanner(stream)
			if translator.NeedConvert(handlerType, c.Type()) {
				for scanner.Scan() {
					line := scanner.Bytes()
					if bytes.HasPrefix(line, dataTag) {
						lines := translator.Response(handlerType, c.Type(), newCtx, modelName, originalRequestRawJSON, rawJSON, line[6:], &param)
						for i := 0; i < len(lines); i++ {
							dataChan <- []byte(lines[i])
						}
					}
					c.AddAPIResponseData(ctx, line)
				}
			} else {
				for scanner.Scan() {
					line := scanner.Bytes()
					if bytes.HasPrefix(line, dataTag) {
						dataChan <- line[6:]
					}
					c.AddAPIResponseData(ctx, line)
				}
			}

			if errScanner := scanner.Err(); errScanner != nil {
				errChan <- &interfaces.ErrorMessage{StatusCode: 500, Error: errScanner}
				_ = stream.Close()
				return
			}

		} else {
			data, errReadAll := io.ReadAll(stream)
			if errReadAll != nil {
				errChan <- &interfaces.ErrorMessage{StatusCode: 500, Error: errReadAll}
				_ = stream.Close()
				return
			}

			if translator.NeedConvert(handlerType, c.Type()) {
				lines := translator.Response(handlerType, c.Type(), newCtx, modelName, originalRequestRawJSON, rawJSON, data, &param)
				for i := 0; i < len(lines); i++ {
					dataChan <- []byte(lines[i])
				}
			} else {
				dataChan <- data
			}

			c.AddAPIResponseData(ctx, data)
		}

		if translator.NeedConvert(handlerType, c.Type()) {
			lines := translator.Response(handlerType, c.Type(), ctx, modelName, rawJSON, originalRequestRawJSON, []byte("[DONE]"), &param)
			for i := 0; i < len(lines); i++ {
				dataChan <- []byte(lines[i])
			}
		}

		_ = stream.Close()

	}()

	return dataChan, errChan
}

// IsModelQuotaExceeded returns true if the specified model has exceeded its quota
// and no fallback options are available.
//
// Parameters:
//   - model: The name of the model to check.
//
// Returns:
//   - bool: True if the model's quota is exceeded, false otherwise.
func (c *GeminiClient) IsModelQuotaExceeded(model string) bool {
	if lastExceededTime, hasKey := c.modelQuotaExceeded[model]; hasKey {
		duration := time.Now().Sub(*lastExceededTime)
		if duration > 30*time.Minute {
			return false
		}
		return true
	}
	return false
}

// SaveTokenToFile serializes the client's current token storage to a JSON file.
// The filename is constructed from the user's email and project ID.
//
// Returns:
//   - error: Always nil for this implementation.
func (c *GeminiClient) SaveTokenToFile() error {
	return nil
}

// GetUserAgent constructs the User-Agent string for HTTP requests.
func (c *GeminiClient) GetUserAgent() string {
	// return fmt.Sprintf("GeminiCLI/%s (%s; %s)", pluginVersion, runtime.GOOS, runtime.GOARCH)
	return "google-api-nodejs-client/9.15.1"
}

// GetRequestMutex returns the mutex used to synchronize requests for this client.
// This ensures that only one request is processed at a time for quota management.
//
// Returns:
//   - *sync.Mutex: The mutex used for request synchronization
func (c *GeminiClient) GetRequestMutex() *sync.Mutex {
	return nil
}

func (c *GeminiClient) RefreshTokens(ctx context.Context) error {
	// API keys don't need refreshing
	return nil
}
