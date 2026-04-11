package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Role string

const (
	RoleAdmin      Role = "admin"
	RoleTenantUser Role = "tenant_user"
	RoleService    Role = "service"
)

var (
	ErrMissingBearerToken = errors.New("missing bearer token")
	ErrInvalidBearerToken = errors.New("invalid bearer token")
)

type Claims struct {
	Role     Role   `json:"role"`
	TenantID string `json:"tenant_id,omitempty"`
	jwt.RegisteredClaims
}

func (c Claims) Validate() error {
	switch c.Role {
	case RoleAdmin:
		return nil
	case RoleTenantUser:
		if strings.TrimSpace(c.TenantID) == "" {
			return errors.New("tenant_user token requires tenant_id")
		}
		return nil
	default:
		return fmt.Errorf("unsupported role %q", c.Role)
	}
}

type Principal struct {
	Role     Role
	TenantID string
	Subject  string
}

type contextKey struct{}

func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(contextKey{}).(Principal)
	return principal, ok
}

type Verifier struct {
	secret []byte
	parser *jwt.Parser
}

func NewVerifier(secret string, issuer string, audience string) (*Verifier, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return nil, errors.New("jwt secret is required")
	}

	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
	}
	if issuer = strings.TrimSpace(issuer); issuer != "" {
		options = append(options, jwt.WithIssuer(issuer))
	}
	if audience = strings.TrimSpace(audience); audience != "" {
		options = append(options, jwt.WithAudience(audience))
	}

	return &Verifier{
		secret: []byte(secret),
		parser: jwt.NewParser(options...),
	}, nil
}

func (v *Verifier) ParseRequest(r *http.Request) (Principal, error) {
	token := BearerToken(r.Header.Get("Authorization"))
	if token == "" {
		return Principal{}, ErrMissingBearerToken
	}
	return v.ParseToken(token)
}

func (v *Verifier) ParseToken(tokenString string) (Principal, error) {
	claims := &Claims{}
	token, err := v.parser.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
		return v.secret, nil
	})
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidBearerToken, err)
	}
	if !token.Valid {
		return Principal{}, ErrInvalidBearerToken
	}
	if err := claims.Validate(); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidBearerToken, err)
	}

	return Principal{
		Role:     claims.Role,
		TenantID: strings.TrimSpace(claims.TenantID),
		Subject:  claims.Subject,
	}, nil
}

func NewClaims(role Role, tenantID string, subject string, issuer string, audience string, expiresAt time.Time) Claims {
	claims := Claims{
		Role:     role,
		TenantID: strings.TrimSpace(tenantID),
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strings.TrimSpace(subject),
			Issuer:    strings.TrimSpace(issuer),
			ExpiresAt: jwt.NewNumericDate(expiresAt.UTC()),
			IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
		},
	}
	if audience = strings.TrimSpace(audience); audience != "" {
		claims.Audience = jwt.ClaimStrings{audience}
	}
	return claims
}

func SignHS256(secret string, claims Claims) (string, error) {
	if secret = strings.TrimSpace(secret); secret == "" {
		return "", errors.New("jwt secret is required")
	}
	if err := claims.Validate(); err != nil {
		return "", err
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

func BearerToken(header string) string {
	header = strings.TrimSpace(header)
	if len(header) < 8 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}
