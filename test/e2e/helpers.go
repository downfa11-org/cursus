package e2e

import (
	"fmt"
	"net/http"
	"time"
)

const (
	healthCheckURL      = "http://localhost:9080/health"
	healthCheckRetries  = 30
	healthCheckInterval = 1 * time.Second
)

// CheckBrokerHealth verifies broker is ready (assumes already started by Makefile)
func CheckBrokerHealth() error {
	for i := 0; i < healthCheckRetries; i++ {
		resp, err := http.Get(healthCheckURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(healthCheckInterval)
	}
	return fmt.Errorf("broker not ready after %d attempts", healthCheckRetries)
}
