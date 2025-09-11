// Package management provides the management API handlers and middleware
// for configuring the server and managing auth files.
package management

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/luispater/CLIProxyAPI/internal/config"
	"golang.org/x/crypto/bcrypt"
)

type attemptInfo struct {
	count        int
	blockedUntil time.Time
}

// Handler aggregates config reference, persistence path and helpers.
type Handler struct {
	cfg            *config.Config
	configFilePath string
	mu             sync.Mutex

	attemptsMu     sync.Mutex
	failedAttempts map[string]*attemptInfo // keyed by client IP
}

// NewHandler creates a new management handler instance.
func NewHandler(cfg *config.Config, configFilePath string) *Handler {
	return &Handler{cfg: cfg, configFilePath: configFilePath, failedAttempts: make(map[string]*attemptInfo)}
}

// SetConfig updates the in-memory config reference when the server hot-reloads.
func (h *Handler) SetConfig(cfg *config.Config) { h.cfg = cfg }

// Middleware enforces access control for management endpoints.
// All requests (local and remote) require a valid management key.
// Additionally, remote access requires allow-remote-management=true.
func (h *Handler) Middleware() gin.HandlerFunc {
	const maxFailures = 5
	const banDuration = 30 * time.Minute

	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		// For remote IPs, enforce allow-remote-management and ban checks
		if !(clientIP == "127.0.0.1" || clientIP == "::1") {
			// Check if IP is currently blocked
			h.attemptsMu.Lock()
			ai := h.failedAttempts[clientIP]
			if ai != nil {
				if !ai.blockedUntil.IsZero() {
					if time.Now().Before(ai.blockedUntil) {
						remaining := time.Until(ai.blockedUntil).Round(time.Second)
						h.attemptsMu.Unlock()
						c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("IP banned due to too many failed attempts. Try again in %s", remaining)})
						return
					}
					// Ban expired, reset state
					ai.blockedUntil = time.Time{}
					ai.count = 0
				}
			}
			h.attemptsMu.Unlock()

			allowRemote := h.cfg.RemoteManagement.AllowRemote
			if !allowRemote {
				allowRemote = true
			}
			if !allowRemote {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "remote management disabled"})
				return
			}
		}
		secret := h.cfg.RemoteManagement.SecretKey
		if secret == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "remote management key not set"})
			return
		}

		// Accept either Authorization: Bearer <key> or X-Management-Key
		var provided string
		if ah := c.GetHeader("Authorization"); ah != "" {
			parts := strings.SplitN(ah, " ", 2)
			if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
				provided = parts[1]
			} else {
				provided = ah
			}
		}
		if provided == "" {
			provided = c.GetHeader("X-Management-Key")
		}

		if !(clientIP == "127.0.0.1" || clientIP == "::1") {
			// For remote IPs, enforce key and track failures
			fail := func() {
				h.attemptsMu.Lock()
				ai := h.failedAttempts[clientIP]
				if ai == nil {
					ai = &attemptInfo{}
					h.failedAttempts[clientIP] = ai
				}
				ai.count++
				if ai.count >= maxFailures {
					ai.blockedUntil = time.Now().Add(banDuration)
					ai.count = 0
				}
				h.attemptsMu.Unlock()
			}

			if provided == "" {
				fail()
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing management key"})
				return
			}

			if err := bcrypt.CompareHashAndPassword([]byte(secret), []byte(provided)); err != nil {
				fail()
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid management key"})
				return
			}

			// Success: reset failed count for this IP
			h.attemptsMu.Lock()
			if ai := h.failedAttempts[clientIP]; ai != nil {
				ai.count = 0
				ai.blockedUntil = time.Time{}
			}
			h.attemptsMu.Unlock()
		}

		c.Next()
	}
}

// persist saves the current in-memory config to disk.
func (h *Handler) persist(c *gin.Context) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Preserve comments when writing
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to save config: %v", err)})
		return false
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
	return true
}

// Helper methods for simple types
func (h *Handler) updateBoolField(c *gin.Context, set func(bool)) {
	var body struct {
		Value *bool `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		var m map[string]any
		if err2 := c.ShouldBindJSON(&m); err2 == nil {
			for _, v := range m {
				if b, ok := v.(bool); ok {
					set(b)
					h.persist(c)
					return
				}
			}
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	set(*body.Value)
	h.persist(c)
}

func (h *Handler) updateIntField(c *gin.Context, set func(int)) {
	var body struct {
		Value *int `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	set(*body.Value)
	h.persist(c)
}

func (h *Handler) updateStringField(c *gin.Context, set func(string)) {
	var body struct {
		Value *string `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	set(*body.Value)
	h.persist(c)
}
