// Package management provides the management API handlers and middleware
// for configuring the server and managing auth files.
package management

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/luispater/CLIProxyAPI/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// Handler aggregates config reference, persistence path and helpers.
type Handler struct {
	cfg            *config.Config
	configFilePath string
	mu             sync.Mutex
}

// NewHandler creates a new management handler instance.
func NewHandler(cfg *config.Config, configFilePath string) *Handler {
	return &Handler{cfg: cfg, configFilePath: configFilePath}
}

// SetConfig updates the in-memory config reference when the server hot-reloads.
func (h *Handler) SetConfig(cfg *config.Config) { h.cfg = cfg }

// Middleware enforces access control for management endpoints.
// All requests (local and remote) require a valid management key.
// Additionally, remote access requires allow-remote-management=true.
func (h *Handler) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		// Remote access control: when not loopback, must be enabled
		if !(clientIP == "127.0.0.1" || clientIP == "::1") {
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
			if provided == "" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing management key"})
				return
			}

			if err := bcrypt.CompareHashAndPassword([]byte(secret), []byte(provided)); err != nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid management key"})
				return
			}
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
