package authentication

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

func TestJWTMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		setupAuth      func() string
		expectedStatus int
		expectedError  string
		shouldSetUser  bool
	}{
		{
			name: "valid token with Bearer prefix",
			setupAuth: func() string {
				token := createValidToken("user123")
				return "Bearer " + token
			},
			expectedStatus: 200,
			shouldSetUser:  true,
		},
		{
			name: "missing authorization header",
			setupAuth: func() string {
				return ""
			},
			expectedStatus: 401,
			expectedError:  "missing authorization header",
		},
		{
			name: "malformed authorization header",
			setupAuth: func() string {
				return "invalidFormat token123"
			},
			expectedStatus: 401,
			expectedError:  "invalid authorization header format",
		},
		{
			name: "expired token",
			setupAuth: func() string {
				token := createExpiredToken("user123")
				return "Bearer " + token
			},
			expectedStatus: 401,
			expectedError:  "token expired",
		},
		{
			name: "token with wrong signing method",
			setupAuth: func() string {
				token := createTokenWithWrongAlgorithm("user123")
				return "Bearer " + token
			},
			expectedStatus: 401,
			expectedError:  "invalid signing method",
		},
		{
			name: "token signed with wrong secret",
			setupAuth: func() string {
				token := createTokenWithWrongSecret("user123")
				return "Bearer " + token
			},
			expectedStatus: 401,
			expectedError:  "invalid token signature",
		},
		{
			name: "token without required claims",
			setupAuth: func() string {
				token := createTokenWithoutRequiredClaims()
				return "Bearer " + token
			},
			expectedStatus: 401,
			expectedError:  "missing required claims",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.Use(JWTMiddleware(Config{
				SigningKey:    []byte("test-secret-key"),
				SigningMethod: jwt.SigningMethodHS256,
			}))

			router.GET("/protected", func(c *gin.Context) {
				userID, exists := c.Get("user_id")
				if exists {
					c.JSON(200, gin.H{"user_id": userID})
				} else {
					c.JSON(500, gin.H{"error": "user ID not set"})
				}
			})

			req := httptest.NewRequest("GET", "/protected", nil)
			if authHeader := test.setupAuth(); authHeader != "" {
				req.Header.Set("Authorization", authHeader)
			}

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)

			assert.Equal(t, test.expectedStatus, recorder.Code)

			if test.expectedError != "" {
				assert.Contains(t, recorder.Body.String(), test.expectedError)
			}

			if test.shouldSetUser {
				assert.Contains(t, recorder.Body.String(), "user_id")
			}
		})
	}
}

func createValidToken(userID string) string {
	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour).Unix(),
		"iat":     time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret-key"))
	return tokenString
}

func createExpiredToken(userID string) string {
	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(-time.Hour).Unix(), // Expired 1 hour ago
		"iat":     time.Now().Add(-2 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret-key"))
	return tokenString
}

func createTokenWithWrongAlgorithm(userID string) string {
	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour).Unix(),
	}
	// Using HS512 instead of expected HS256
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	tokenString, _ := token.SignedString([]byte("test-secret-key"))
	return tokenString
}

func createTokenWithWrongSecret(userID string) string {
	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("wrong-secret"))
	return tokenString
}

func createTokenWithoutRequiredClaims() string {
	claims := jwt.MapClaims{
		"exp": time.Now().Add(time.Hour).Unix(),
		// Missing user_id claim
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret-key"))
	return tokenString
}
