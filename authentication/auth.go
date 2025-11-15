package authentication

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

var ErrInvalidSigningMethod = errors.New("invalid signing method")

type Config struct {
	SigningKey    []byte
	SigningMethod jwt.SigningMethod
}

func JWTMiddleware(config Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			c.Abort()
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header format"})
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
			var errorMsg string
			switch {
			case err.Error() == "token has invalid claims: token is expired":
				errorMsg = "token expired"
			case errors.Is(err, ErrInvalidSigningMethod) || strings.Contains(err.Error(), "invalid signing method"):
				errorMsg = "invalid signing method"
			case strings.Contains(err.Error(), "signature is invalid"):
				errorMsg = "invalid token signature"
			default:
				errorMsg = "invalid token signature"
			}
			c.JSON(http.StatusUnauthorized, gin.H{"error": errorMsg})
			c.Abort()
			return
		}

		if !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token signature"})
			c.Abort()
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token signature"})
			c.Abort()
			return
		}

		userID, exists := claims["user_id"]
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "missing required claims"})
			c.Abort()
			return
		}

		c.Set("user_id", userID)
		c.Next()
	}
}
