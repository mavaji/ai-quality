package authentication

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

var ErrInvalidSigningMethod = errors.New("invalid signing method")
var ErrMissingAuthHeader = errors.New("missing authorization header")
var ErrInvalidAuthHeaderFormat = errors.New("invalid authorization header format")
var ErrInvalidTokenSignature = errors.New("invalid token signature")
var ErrTokenExpired = errors.New("token expired")
var ErrMissingRequiredClaims = errors.New("missing required claims")

type Config struct {
	SigningKey    []byte
	SigningMethod jwt.SigningMethod
}

func JWTMiddleware(config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrMissingAuthHeader.Error()})
			c.Abort()
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrInvalidAuthHeaderFormat.Error()})
			c.Abort()
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if token.Method != config.SigningMethod {
				return nil, ErrInvalidSigningMethod
			}
			return config.SigningKey, nil
		})

		if err != nil {
			var authError error
			switch {
			case strings.Contains(err.Error(), "token has invalid claims: token is expired"):
				authError = ErrTokenExpired
			case strings.Contains(err.Error(), "invalid signing method"):
				authError = ErrInvalidSigningMethod
			case strings.Contains(err.Error(), "signature is invalid"):
				authError = ErrInvalidTokenSignature
			default:
				authError = ErrInvalidTokenSignature
			}
			c.JSON(http.StatusUnauthorized, gin.H{"error": authError.Error()})
			c.Abort()
			return
		}

		if !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrInvalidTokenSignature.Error()})
			c.Abort()
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrInvalidTokenSignature.Error()})
			c.Abort()
			return
		}

		userID, exists := claims["user_id"]
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrMissingRequiredClaims.Error()})
			c.Abort()
			return
		}

		c.Set("user_id", userID)
		c.Next()
	}
}
