package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

type ClusterClient interface {
	JoinCluster(peers []string, nodeID, addr string, discoveryPort int) error
}

type HTTPClusterClient struct{}

func (c *HTTPClusterClient) JoinCluster(peers []string, nodeID, addr string, discoveryPort int) error {
	apiPort := discoveryPort
	if apiPort == 0 {
		apiPort = 8000
	}

	seedHosts := c.extractSeedHosts(peers, addr)
	if len(seedHosts) == 0 {
		util.Warn("No seed hosts available for join; aborting join attempts")
		return fmt.Errorf("no seed hosts available")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	maxAttempts := 8
	backoff := time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.attemptJoin(client, seedHosts, nodeID, addr, apiPort, attempt, maxAttempts); err != nil {
			util.Warn("Join attempt %d failed: %v", attempt, err)
		} else {
			return nil
		}
		time.Sleep(backoff)
		backoff *= 2
	}

	return fmt.Errorf("failed to join cluster after %d attempts", maxAttempts)
}

func (c *HTTPClusterClient) extractSeedHosts(peers []string, localAddr string) []string {
	seedHosts := make([]string, 0, len(peers))
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		var addrOnly string
		if strings.Contains(p, "@") {
			parts := strings.SplitN(p, "@", 2)
			if len(parts) == 2 {
				addrOnly = parts[1]
			} else {
				addrOnly = p
			}
		} else {
			addrOnly = p
		}

		if addrOnly != localAddr {
			seedHosts = append(seedHosts, addrOnly)
		}
	}
	return seedHosts
}

func (c *HTTPClusterClient) attemptJoin(client *http.Client, seedHosts []string, nodeID, addr string, apiPort, attempt, maxAttempts int) error {
	payload := map[string]string{
		"node_id": nodeID,
		"address": addr,
	}
	body, _ := json.Marshal(payload)

	for _, seed := range seedHosts {
		hostOnly := seed
		if strings.Contains(seed, ":") {
			hostOnly = strings.Split(seed, ":")[0]
		}
		joinURL := fmt.Sprintf("http://%s:%d/join", hostOnly, apiPort)

		util.Info("attempting cluster join to %s (attempt %d/%d)", joinURL, attempt, maxAttempts)

		req, _ := http.NewRequest(http.MethodPost, joinURL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			util.Warn("join request to %s failed: %v", joinURL, err)
			continue
		}

		var jr struct {
			Success bool   `json:"success"`
			Leader  string `json:"leader"`
			Error   string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&jr)
		resp.Body.Close()

		if jr.Success {
			util.Info("join succeeded via %s; leader=%s", joinURL, jr.Leader)
			return nil
		}

		if jr.Leader != "" {
			if c.retryWithLeader(client, jr.Leader, apiPort, body) == nil {
				return nil
			}
		}
	}

	return fmt.Errorf("all join attempts failed")
}

func (c *HTTPClusterClient) retryWithLeader(client *http.Client, leader string, apiPort int, body []byte) error {
	leaderHost := leader
	if strings.Contains(leader, ":") {
		leaderHost = strings.Split(leader, ":")[0]
	}

	leaderJoinURL := fmt.Sprintf("http://%s:%d/join", leaderHost, apiPort)
	util.Info("retrying join at leader %s", leaderJoinURL)

	req, _ := http.NewRequest(http.MethodPost, leaderJoinURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		util.Warn("join request to leader %s failed: %v", leaderJoinURL, err)
		return err
	}
	defer resp.Body.Close()

	var jr struct {
		Success bool   `json:"success"`
		Leader  string `json:"leader"`
		Error   string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&jr)

	if jr.Success {
		util.Info("join succeeded via leader %s", leaderJoinURL)
		return nil
	}

	util.Warn("leader join attempt failed: %v (leader=%s)", jr.Error, jr.Leader)
	return fmt.Errorf("leader join failed: %s", jr.Error)
}
