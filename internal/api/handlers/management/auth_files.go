package management

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/luispater/CLIProxyAPI/internal/auth/claude"
	"github.com/luispater/CLIProxyAPI/internal/auth/codex"
	geminiAuth "github.com/luispater/CLIProxyAPI/internal/auth/gemini"
	"github.com/luispater/CLIProxyAPI/internal/auth/qwen"
	"github.com/luispater/CLIProxyAPI/internal/client"
	"github.com/luispater/CLIProxyAPI/internal/misc"
	"github.com/luispater/CLIProxyAPI/internal/util"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var (
	oauthStatus = make(map[string]string)
)

// List auth files
func (h *Handler) ListAuthFiles(c *gin.Context) {
	entries, err := os.ReadDir(h.cfg.AuthDir)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", err)})
		return
	}
	files := make([]gin.H, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		if info, errInfo := e.Info(); errInfo == nil {
			fileData := gin.H{"name": name, "size": info.Size(), "modtime": info.ModTime()}

			// Read file to get type field
			full := filepath.Join(h.cfg.AuthDir, name)
			if data, errRead := os.ReadFile(full); errRead == nil {
				typeValue := gjson.GetBytes(data, "type").String()
				fileData["type"] = typeValue
			}

			files = append(files, fileData)
		}
	}
	c.JSON(200, gin.H{"files": files})
}

// Download single auth file by name
func (h *Handler) DownloadAuthFile(c *gin.Context) {
	name := c.Query("name")
	if name == "" || strings.Contains(name, string(os.PathSeparator)) {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	if !strings.HasSuffix(strings.ToLower(name), ".json") {
		c.JSON(400, gin.H{"error": "name must end with .json"})
		return
	}
	full := filepath.Join(h.cfg.AuthDir, name)
	data, err := os.ReadFile(full)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(404, gin.H{"error": "file not found"})
		} else {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read file: %v", err)})
		}
		return
	}
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", name))
	c.Data(200, "application/json", data)
}

// Upload auth file: multipart or raw JSON with ?name=
func (h *Handler) UploadAuthFile(c *gin.Context) {
	if file, err := c.FormFile("file"); err == nil && file != nil {
		name := filepath.Base(file.Filename)
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			c.JSON(400, gin.H{"error": "file must be .json"})
			return
		}
		dst := filepath.Join(h.cfg.AuthDir, name)
		if errSave := c.SaveUploadedFile(file, dst); errSave != nil {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to save file: %v", errSave)})
			return
		}
		c.JSON(200, gin.H{"status": "ok"})
		return
	}
	name := c.Query("name")
	if name == "" || strings.Contains(name, string(os.PathSeparator)) {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	if !strings.HasSuffix(strings.ToLower(name), ".json") {
		c.JSON(400, gin.H{"error": "name must end with .json"})
		return
	}
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"error": "failed to read body"})
		return
	}
	dst := filepath.Join(h.cfg.AuthDir, filepath.Base(name))
	if errWrite := os.WriteFile(dst, data, 0o600); errWrite != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to write file: %v", errWrite)})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}

// Delete auth files: single by name or all
func (h *Handler) DeleteAuthFile(c *gin.Context) {
	if all := c.Query("all"); all == "true" || all == "1" || all == "*" {
		entries, err := os.ReadDir(h.cfg.AuthDir)
		if err != nil {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", err)})
			return
		}
		deleted := 0
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasSuffix(strings.ToLower(name), ".json") {
				continue
			}
			full := filepath.Join(h.cfg.AuthDir, name)
			if err = os.Remove(full); err == nil {
				deleted++
			}
		}
		c.JSON(200, gin.H{"status": "ok", "deleted": deleted})
		return
	}
	name := c.Query("name")
	if name == "" || strings.Contains(name, string(os.PathSeparator)) {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	full := filepath.Join(h.cfg.AuthDir, filepath.Base(name))
	if err := os.Remove(full); err != nil {
		if os.IsNotExist(err) {
			c.JSON(404, gin.H{"error": "file not found"})
		} else {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to remove file: %v", err)})
		}
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}

func (h *Handler) RequestAnthropicToken(c *gin.Context) {
	ctx := context.Background()

	log.Info("Initializing Claude authentication...")

	// Generate PKCE codes
	pkceCodes, err := claude.GeneratePKCECodes()
	if err != nil {
		log.Fatalf("Failed to generate PKCE codes: %v", err)
		return
	}

	// Generate random state parameter
	state, err := misc.GenerateRandomState()
	if err != nil {
		log.Fatalf("Failed to generate state parameter: %v", err)
		return
	}

	// Initialize Claude auth service
	anthropicAuth := claude.NewClaudeAuth(h.cfg)

	// Generate authorization URL (then override redirect_uri to reuse server port)
	authURL, state, err := anthropicAuth.GenerateAuthURL(state, pkceCodes)
	if err != nil {
		log.Fatalf("Failed to generate authorization URL: %v", err)
		return
	}
	// Override redirect_uri in authorization URL to current server port

	go func() {
		// Helper: wait for callback file
		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-anthropic-%s.oauth", state))
		waitForFile := func(path string, timeout time.Duration) (map[string]string, error) {
			deadline := time.Now().Add(timeout)
			for {
				if time.Now().After(deadline) {
					oauthStatus[state] = "Timeout waiting for OAuth callback"
					return nil, fmt.Errorf("timeout waiting for OAuth callback")
				}
				data, errRead := os.ReadFile(path)
				if errRead == nil {
					var m map[string]string
					_ = json.Unmarshal(data, &m)
					_ = os.Remove(path)
					return m, nil
				}
				time.Sleep(500 * time.Millisecond)
			}
		}

		log.Info("Waiting for authentication callback...")
		// Wait up to 5 minutes
		resultMap, errWait := waitForFile(waitFile, 5*time.Minute)
		if errWait != nil {
			authErr := claude.NewAuthenticationError(claude.ErrCallbackTimeout, errWait)
			log.Error(claude.GetUserFriendlyMessage(authErr))
			return
		}
		if errStr := resultMap["error"]; errStr != "" {
			oauthErr := claude.NewOAuthError(errStr, "", http.StatusBadRequest)
			log.Error(claude.GetUserFriendlyMessage(oauthErr))
			oauthStatus[state] = "Bad request"
			return
		}
		if resultMap["state"] != state {
			authErr := claude.NewAuthenticationError(claude.ErrInvalidState, fmt.Errorf("expected %s, got %s", state, resultMap["state"]))
			log.Error(claude.GetUserFriendlyMessage(authErr))
			oauthStatus[state] = "State code error"
			return
		}

		// Parse code (Claude may append state after '#')
		rawCode := resultMap["code"]
		code := strings.Split(rawCode, "#")[0]

		// Exchange code for tokens (replicate logic using updated redirect_uri)
		// Extract client_id from the modified auth URL
		clientID := ""
		if u2, errP := url.Parse(authURL); errP == nil {
			clientID = u2.Query().Get("client_id")
		}
		// Build request
		bodyMap := map[string]any{
			"code":          code,
			"state":         state,
			"grant_type":    "authorization_code",
			"client_id":     clientID,
			"redirect_uri":  "http://localhost:54545/callback",
			"code_verifier": pkceCodes.CodeVerifier,
		}
		bodyJSON, _ := json.Marshal(bodyMap)

		httpClient := util.SetProxy(h.cfg, &http.Client{})
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://console.anthropic.com/v1/oauth/token", strings.NewReader(string(bodyJSON)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, errDo := httpClient.Do(req)
		if errDo != nil {
			authErr := claude.NewAuthenticationError(claude.ErrCodeExchangeFailed, errDo)
			log.Errorf("Failed to exchange authorization code for tokens: %v", authErr)
			oauthStatus[state] = "Failed to exchange authorization code for tokens"
			return
		}
		defer func() {
			if errClose := resp.Body.Close(); errClose != nil {
				log.Errorf("failed to close response body: %v", errClose)
			}
		}()
		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			log.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(respBody))
			oauthStatus[state] = fmt.Sprintf("token exchange failed with status %d", resp.StatusCode)
			return
		}
		var tResp struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int    `json:"expires_in"`
			Account      struct {
				EmailAddress string `json:"email_address"`
			} `json:"account"`
		}
		if errU := json.Unmarshal(respBody, &tResp); errU != nil {
			log.Errorf("failed to parse token response: %v", errU)
			oauthStatus[state] = "Failed to parse token response"
			return
		}
		bundle := &claude.ClaudeAuthBundle{
			TokenData: claude.ClaudeTokenData{
				AccessToken:  tResp.AccessToken,
				RefreshToken: tResp.RefreshToken,
				Email:        tResp.Account.EmailAddress,
				Expire:       time.Now().Add(time.Duration(tResp.ExpiresIn) * time.Second).Format(time.RFC3339),
			},
			LastRefresh: time.Now().Format(time.RFC3339),
		}

		// Create token storage
		tokenStorage := anthropicAuth.CreateTokenStorage(bundle)
		// Initialize Claude client
		anthropicClient := client.NewClaudeClient(h.cfg, tokenStorage)
		// Save token storage
		if errSave := anthropicClient.SaveTokenToFile(); errSave != nil {
			log.Fatalf("Failed to save authentication tokens: %v", errSave)
			oauthStatus[state] = "Failed to save authentication tokens"
			return
		}

		log.Info("Authentication successful!")
		if bundle.APIKey != "" {
			log.Info("API key obtained and saved")
		}
		log.Info("You can now use Claude services through this CLI")
		delete(oauthStatus, state)
	}()

	oauthStatus[state] = ""
	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func (h *Handler) RequestGeminiCLIToken(c *gin.Context) {
	ctx := context.Background()

	// Optional project ID from query
	projectID := c.Query("project_id")

	log.Info("Initializing Google authentication...")

	// OAuth2 configuration (mirrors internal/auth/gemini)
	conf := &oauth2.Config{
		ClientID:     "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com",
		ClientSecret: "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",
		RedirectURL:  "http://localhost:8085/oauth2callback",
		Scopes: []string{
			"https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/userinfo.email",
			"https://www.googleapis.com/auth/userinfo.profile",
		},
		Endpoint: google.Endpoint,
	}

	// Build authorization URL and return it immediately
	state := fmt.Sprintf("gem-%d", time.Now().UnixNano())
	authURL := conf.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.SetAuthURLParam("prompt", "consent"))

	go func() {
		// Wait for callback file written by server route
		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-gemini-%s.oauth", state))
		log.Info("Waiting for authentication callback...")
		deadline := time.Now().Add(5 * time.Minute)
		var authCode string
		for {
			if time.Now().After(deadline) {
				log.Error("oauth flow timed out")
				oauthStatus[state] = "OAuth flow timed out"
				return
			}
			if data, errR := os.ReadFile(waitFile); errR == nil {
				var m map[string]string
				_ = json.Unmarshal(data, &m)
				_ = os.Remove(waitFile)
				if errStr := m["error"]; errStr != "" {
					log.Errorf("Authentication failed: %s", errStr)
					oauthStatus[state] = "Authentication failed"
					return
				}
				authCode = m["code"]
				if authCode == "" {
					log.Errorf("Authentication failed: code not found")
					oauthStatus[state] = "Authentication failed: code not found"
					return
				}
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		// Exchange authorization code for token
		token, err := conf.Exchange(ctx, authCode)
		if err != nil {
			log.Errorf("Failed to exchange token: %v", err)
			oauthStatus[state] = "Failed to exchange token"
			return
		}

		// Create token storage (mirrors internal/auth/gemini createTokenStorage)
		httpClient := conf.Client(ctx, token)
		req, errNewRequest := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/oauth2/v1/userinfo?alt=json", nil)
		if errNewRequest != nil {
			log.Errorf("Could not get user info: %v", errNewRequest)
			oauthStatus[state] = "Could not get user info"
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))

		resp, errDo := httpClient.Do(req)
		if errDo != nil {
			log.Errorf("Failed to execute request: %v", errDo)
			oauthStatus[state] = "Failed to execute request"
			return
		}
		defer func() {
			if errClose := resp.Body.Close(); errClose != nil {
				log.Printf("warn: failed to close response body: %v", errClose)
			}
		}()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Errorf("Get user info request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
			oauthStatus[state] = fmt.Sprintf("Get user info request failed with status %d", resp.StatusCode)
			return
		}

		email := gjson.GetBytes(bodyBytes, "email").String()
		if email != "" {
			log.Infof("Authenticated user email: %s", email)
		} else {
			log.Info("Failed to get user email from token")
			oauthStatus[state] = "Failed to get user email from token"
		}

		// Marshal/unmarshal oauth2.Token to generic map and enrich fields
		var ifToken map[string]any
		jsonData, _ := json.Marshal(token)
		if errUnmarshal := json.Unmarshal(jsonData, &ifToken); errUnmarshal != nil {
			log.Errorf("Failed to unmarshal token: %v", errUnmarshal)
			oauthStatus[state] = "Failed to unmarshal token"
			return
		}

		ifToken["token_uri"] = "https://oauth2.googleapis.com/token"
		ifToken["client_id"] = "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com"
		ifToken["client_secret"] = "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"
		ifToken["scopes"] = []string{
			"https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/userinfo.email",
			"https://www.googleapis.com/auth/userinfo.profile",
		}
		ifToken["universe_domain"] = "googleapis.com"

		ts := geminiAuth.GeminiTokenStorage{
			Token:     ifToken,
			ProjectID: projectID,
			Email:     email,
		}

		// Initialize authenticated HTTP client via GeminiAuth to honor proxy settings
		gemAuth := geminiAuth.NewGeminiAuth()
		httpClient2, errGetClient := gemAuth.GetAuthenticatedClient(ctx, &ts, h.cfg, true)
		if errGetClient != nil {
			log.Fatalf("failed to get authenticated client: %v", errGetClient)
			oauthStatus[state] = "Failed to get authenticated client"
			return
		}
		log.Info("Authentication successful.")

		// Initialize the API client
		cliClient := client.NewGeminiCLIClient(httpClient2, &ts, h.cfg)

		// Perform the user setup process (migrated from DoLogin)
		if err = cliClient.SetupUser(ctx, ts.Email, projectID); err != nil {
			if err.Error() == "failed to start user onboarding, need define a project id" {
				log.Error("Failed to start user onboarding: A project ID is required.")
				oauthStatus[state] = "Failed to start user onboarding: A project ID is required"
				project, errGetProjectList := cliClient.GetProjectList(ctx)
				if errGetProjectList != nil {
					log.Fatalf("Failed to get project list: %v", err)
					oauthStatus[state] = "Failed to get project list"
				} else {
					log.Infof("Your account %s needs to specify a project ID.", ts.Email)
					log.Info("========================================================================")
					for _, p := range project.Projects {
						log.Infof("Project ID: %s", p.ProjectID)
						log.Infof("Project Name: %s", p.Name)
						log.Info("------------------------------------------------------------------------")
					}
					log.Infof("Please run this command to login again with a specific project:\n\n%s --login --project_id <project_id>\n", os.Args[0])
				}
			} else {
				log.Fatalf("Failed to complete user setup: %v", err)
				oauthStatus[state] = "Failed to complete user setup"
			}
			return
		}

		// Post-setup checks and token persistence
		auto := projectID == ""
		cliClient.SetIsAuto(auto)
		if !cliClient.IsChecked() && !cliClient.IsAuto() {
			isChecked, checkErr := cliClient.CheckCloudAPIIsEnabled()
			if checkErr != nil {
				log.Fatalf("Failed to check if Cloud AI API is enabled: %v", checkErr)
				oauthStatus[state] = "Failed to check if Cloud AI API is enabled"
				return
			}
			cliClient.SetIsChecked(isChecked)
			if !isChecked {
				log.Fatal("Failed to check if Cloud AI API is enabled. If you encounter an error message, please create an issue.")
				oauthStatus[state] = "Failed to check if Cloud AI API is enabled"
				return
			}
		}

		if err = cliClient.SaveTokenToFile(); err != nil {
			log.Fatalf("Failed to save token to file: %v", err)
			oauthStatus[state] = "Failed to save token to file"
			return
		}

		delete(oauthStatus, state)
		log.Info("You can now use Gemini CLI services through this CLI")
	}()

	oauthStatus[state] = ""
	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func (h *Handler) RequestCodexToken(c *gin.Context) {
	ctx := context.Background()

	log.Info("Initializing Codex authentication...")

	// Generate PKCE codes
	pkceCodes, err := codex.GeneratePKCECodes()
	if err != nil {
		log.Fatalf("Failed to generate PKCE codes: %v", err)
		return
	}

	// Generate random state parameter
	state, err := misc.GenerateRandomState()
	if err != nil {
		log.Fatalf("Failed to generate state parameter: %v", err)
		return
	}

	// Initialize Codex auth service
	openaiAuth := codex.NewCodexAuth(h.cfg)

	// Generate authorization URL
	authURL, err := openaiAuth.GenerateAuthURL(state, pkceCodes)
	if err != nil {
		log.Fatalf("Failed to generate authorization URL: %v", err)
		return
	}

	go func() {
		// Wait for callback file
		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-codex-%s.oauth", state))
		deadline := time.Now().Add(5 * time.Minute)
		var code string
		for {
			if time.Now().After(deadline) {
				authErr := codex.NewAuthenticationError(codex.ErrCallbackTimeout, fmt.Errorf("timeout waiting for OAuth callback"))
				log.Error(codex.GetUserFriendlyMessage(authErr))
				oauthStatus[state] = "Timeout waiting for OAuth callback"
				return
			}
			if data, errR := os.ReadFile(waitFile); errR == nil {
				var m map[string]string
				_ = json.Unmarshal(data, &m)
				_ = os.Remove(waitFile)
				if errStr := m["error"]; errStr != "" {
					oauthErr := codex.NewOAuthError(errStr, "", http.StatusBadRequest)
					log.Error(codex.GetUserFriendlyMessage(oauthErr))
					oauthStatus[state] = "Bad Request"
					return
				}
				if m["state"] != state {
					authErr := codex.NewAuthenticationError(codex.ErrInvalidState, fmt.Errorf("expected %s, got %s", state, m["state"]))
					oauthStatus[state] = "State code error"
					log.Error(codex.GetUserFriendlyMessage(authErr))
					return
				}
				code = m["code"]
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		log.Debug("Authorization code received, exchanging for tokens...")
		// Extract client_id from authURL
		clientID := ""
		if u2, errP := url.Parse(authURL); errP == nil {
			clientID = u2.Query().Get("client_id")
		}
		// Exchange code for tokens with redirect equal to mgmtRedirect
		form := url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {clientID},
			"code":          {code},
			"redirect_uri":  {"http://localhost:1455/auth/callback"},
			"code_verifier": {pkceCodes.CodeVerifier},
		}
		httpClient := util.SetProxy(h.cfg, &http.Client{})
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://auth.openai.com/oauth/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		resp, errDo := httpClient.Do(req)
		if errDo != nil {
			authErr := codex.NewAuthenticationError(codex.ErrCodeExchangeFailed, errDo)
			oauthStatus[state] = "Failed to exchange authorization code for tokens"
			log.Errorf("Failed to exchange authorization code for tokens: %v", authErr)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			oauthStatus[state] = fmt.Sprintf("Token exchange failed with status %d", resp.StatusCode)
			log.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(respBody))
			return
		}
		var tokenResp struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			IDToken      string `json:"id_token"`
			ExpiresIn    int    `json:"expires_in"`
		}
		if errU := json.Unmarshal(respBody, &tokenResp); errU != nil {
			oauthStatus[state] = "Failed to parse token response"
			log.Errorf("failed to parse token response: %v", errU)
			return
		}
		claims, _ := codex.ParseJWTToken(tokenResp.IDToken)
		email := ""
		accountID := ""
		if claims != nil {
			email = claims.GetUserEmail()
			accountID = claims.GetAccountID()
		}
		// Build bundle compatible with existing storage
		bundle := &codex.CodexAuthBundle{
			TokenData: codex.CodexTokenData{
				IDToken:      tokenResp.IDToken,
				AccessToken:  tokenResp.AccessToken,
				RefreshToken: tokenResp.RefreshToken,
				AccountID:    accountID,
				Email:        email,
				Expire:       time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second).Format(time.RFC3339),
			},
			LastRefresh: time.Now().Format(time.RFC3339),
		}

		// Create token storage and persist
		tokenStorage := openaiAuth.CreateTokenStorage(bundle)
		openaiClient, errInit := client.NewCodexClient(h.cfg, tokenStorage)
		if errInit != nil {
			oauthStatus[state] = "Failed to initialize Codex client"
			log.Fatalf("Failed to initialize Codex client: %v", errInit)
			return
		}
		if errSave := openaiClient.SaveTokenToFile(); errSave != nil {
			oauthStatus[state] = "Failed to save authentication tokens"
			log.Fatalf("Failed to save authentication tokens: %v", errSave)
			return
		}
		log.Info("Authentication successful!")
		if bundle.APIKey != "" {
			log.Info("API key obtained and saved")
		}
		log.Info("You can now use Codex services through this CLI")
		delete(oauthStatus, state)
	}()

	oauthStatus[state] = ""
	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func (h *Handler) RequestQwenToken(c *gin.Context) {
	ctx := context.Background()

	log.Info("Initializing Qwen authentication...")

	state := fmt.Sprintf("gem-%d", time.Now().UnixNano())
	// Initialize Qwen auth service
	qwenAuth := qwen.NewQwenAuth(h.cfg)

	// Generate authorization URL
	deviceFlow, err := qwenAuth.InitiateDeviceFlow(ctx)
	if err != nil {
		log.Fatalf("Failed to generate authorization URL: %v", err)
		return
	}
	authURL := deviceFlow.VerificationURIComplete

	go func() {
		log.Info("Waiting for authentication...")
		tokenData, errPollForToken := qwenAuth.PollForToken(deviceFlow.DeviceCode, deviceFlow.CodeVerifier)
		if errPollForToken != nil {
			oauthStatus[state] = "Authentication failed"
			fmt.Printf("Authentication failed: %v\n", errPollForToken)
			return
		}

		// Create token storage
		tokenStorage := qwenAuth.CreateTokenStorage(tokenData)

		// Initialize Qwen client
		qwenClient := client.NewQwenClient(h.cfg, tokenStorage)

		tokenStorage.Email = fmt.Sprintf("qwen-%d", time.Now().UnixMilli())

		// Save token storage
		if err = qwenClient.SaveTokenToFile(); err != nil {
			log.Fatalf("Failed to save authentication tokens: %v", err)
			oauthStatus[state] = "Failed to save authentication tokens"
			return
		}

		log.Info("Authentication successful!")
		log.Info("You can now use Qwen services through this CLI")
		delete(oauthStatus, state)
	}()

	oauthStatus[state] = ""
	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func (h *Handler) GetAuthStatus(c *gin.Context) {
	state := c.Query("state")
	if err, ok := oauthStatus[state]; ok {
		if err != "" {
			c.JSON(200, gin.H{"status": "error", "error": err})
		} else {
			c.JSON(200, gin.H{"status": "wait"})
			return
		}
	} else {
		c.JSON(200, gin.H{"status": "ok"})
	}
	delete(oauthStatus, state)
}
