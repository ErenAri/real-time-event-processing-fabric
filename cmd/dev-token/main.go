package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"pulsestream/internal/auth"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	role := flag.String("role", string(auth.RoleTenantUser), "token role: admin or tenant_user")
	tenantID := flag.String("tenant", "", "tenant_id claim for tenant_user tokens")
	subject := flag.String("subject", "local-operator", "subject claim")
	secret := flag.String("secret", "", "HMAC signing secret")
	issuer := flag.String("issuer", "pulsestream-local", "issuer claim")
	audience := flag.String("audience", "pulsestream-local", "audience claim")
	expiresIn := flag.Duration("expires-in", 24*time.Hour, "token lifetime")
	flag.Parse()

	expiresAt := time.Now().UTC().Add(*expiresIn)
	claims := auth.NewClaims(auth.Role(*role), *tenantID, *subject, *issuer, *audience, expiresAt)
	token, err := auth.SignHS256(*secret, claims)
	if err != nil {
		return err
	}

	fmt.Println(token)
	return nil
}
