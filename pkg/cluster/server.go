package cluster

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/downfa11-org/go-broker/pkg/cluster/discovery"
	"github.com/downfa11-org/go-broker/util"
)

type ClusterServer struct {
	sd discovery.ServiceDiscovery
}

func NewClusterServer(sd discovery.ServiceDiscovery) *ClusterServer {
	return &ClusterServer{sd: sd}
}

type joinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type joinResponse struct {
	Success bool   `json:"success"`
	Leader  string `json:"leader,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (h *ClusterServer) handleJoinCluster(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(joinResponse{Success: false, Error: "method not allowed"})
		return
	}

	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(joinResponse{Success: false, Error: "invalid json"})
		return
	}
	if req.NodeID == "" || req.Address == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(joinResponse{Success: false, Error: "missing node_id or address"})
		return
	}

	leader, err := h.sd.AddNode(req.NodeID, req.Address)
	if err != nil {
		util.Error("request failed for %s@%s: %v (leader hint: %s)", req.NodeID, req.Address, err, leader)

		resp := joinResponse{
			Success: false,
			Leader:  leader,
			Error:   err.Error(),
		}
		if leader == "" {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusConflict)
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	_ = json.NewEncoder(w).Encode(joinResponse{Success: true, Leader: leader})
}

type leaveReq struct {
	NodeID string `json:"node_id"`
}

type leaveResp struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func (h *ClusterServer) handleLeaveCluster(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(leaveResp{Success: false, Error: "method not allowed"})
		return
	}

	var req leaveReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(leaveResp{Success: false, Error: "invalid request"})
		return
	}

	leader, err := h.sd.RemoveNode(req.NodeID)
	if err != nil {
		log.Printf("remove %s failed: %v (leader hint: %s)\n", req.NodeID, err, leader)
		if leader == "" {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusConflict)
		}
		_ = json.NewEncoder(w).Encode(leaveResp{Success: false, Error: err.Error()})
		return
	}

	_ = json.NewEncoder(w).Encode(leaveResp{Success: true})
}

func (h *ClusterServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/join", h.handleJoinCluster)
	mux.HandleFunc("/leave", h.handleLeaveCluster)
	mux.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		nodes, _ := h.sd.DiscoverBrokers()
		_ = json.NewEncoder(w).Encode(nodes)
	})
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			util.Fatal("cluster server failed: %v", err)
		}
	}()
	return nil
}
