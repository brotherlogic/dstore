package main

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/brotherlogic/dstore/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	write_consensus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dstore_write_consensus",
		Help: "The oldest physical record",
	}, []string{"key"})
	read_consensus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dstore_read_consensus",
		Help: "The oldest physical record",
	}, []string{"key"})
	mainLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dstore_main_latency",
		Help:    "The latency of server requests",
		Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 1024000},
	}, []string{"method"})
	subLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dstore_sub_latency",
		Help:    "The latency of server requests",
		Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 1024000},
	}, []string{"method", "client"})
)

func (s *Server) GetLatest(ctx context.Context, req *pb.GetLatestRequest) (*pb.GetLatestResponse, error) {
	key := "latest"
	if req.GetHash() != "" {
		key = req.GetHash()
	}
	resp, err := s.readFile(req.GetKey(), key)
	if err != nil {
		return nil, err
	}
	return &pb.GetLatestResponse{Hash: resp.GetHash(), Timestamp: resp.GetTimestamp()}, nil
}

//Read reads out some data
func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	t1 := time.Now()
	defer func(run bool) {
		if run {
			mainLatency.With(prometheus.Labels{"method": "READ"}).Observe(float64(time.Since(t1).Milliseconds()))
		}
	}(!req.GetNoFanout())
	//Get the latest item if we don't have hash
	if req.GetHash() == "" {
		req.Hash = "latest"
	}

	resp, err := s.readFile(req.GetKey(), req.GetHash())

	hashMap := make(map[string]*pb.ReadResponse)
	countMap := make(map[string]int)

	bestCount := 1
	bestHash := ""
	friends := []string{"me"}

	if err == nil {
		hashMap[resp.GetHash()] = resp
		countMap[resp.GetHash()] = 1
		bestHash = resp.GetHash()
	}

	fcount := float32(0)
	if !req.NoFanout {
		req.NoFanout = true
		friends, err = s.FFind(ctx, "dstore")
		if err == nil {
			for _, friend := range friends {
				if !strings.HasPrefix(friend, s.Registry.GetIdentifier()) {
					conn, err := s.FDial(friend)
					if err == nil {
						client := pb.NewDStoreServiceClient(conn)

						t2 := time.Now()
						read, err := client.Read(ctx, req)
						subLatency.With(prometheus.Labels{"method": "READ", "client": friend}).Observe(float64(time.Since(t2).Milliseconds()))

						// We only consider reads where we got something back
						if err == nil || status.Convert(err).Code() == codes.InvalidArgument {
							fcount++
						}

						if err == nil {
							if _, ok := hashMap[read.GetHash()]; !ok {
								hashMap[read.GetHash()] = read
							}

							countMap[read.GetHash()]++
							if countMap[read.GetHash()] > bestCount {
								bestCount = countMap[read.GetHash()]
								bestHash = read.GetHash()
							}
						}
						conn.Close()
					}
				}
			}
		}
	}

	// If we've read nothing return not found
	if bestHash == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Cannot locate %v", req.GetKey())
	}

	//Let's get a consensus on the latest
	retResp := hashMap[bestHash]
	retResp.Consensus = float32(bestCount) / fcount

	read_consensus.With(prometheus.Labels{"key": req.GetKey()}).Set(float64(retResp.GetConsensus()))

	return retResp, nil
}

//Write writes out a key
func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if !req.GetNoFanout() {
		s.CtxLog(ctx, fmt.Sprintf("writing %v as main", req.GetKey()))
	}
	t1 := time.Now()
	defer func(run bool) {
		if run {
			mainLatency.With(prometheus.Labels{"method": "WRITE"}).Observe(float64(time.Since(t1).Milliseconds()))
		}
	}(!req.GetNoFanout())
	if strings.HasPrefix(req.GetKey(), "/") {
		return nil, fmt.Errorf("keys should not start with a backslash: %v", req.GetKey())
	}

	s.writeLock.Lock()

	found := false
	for _, k := range s.cleans {
		if k == req.GetKey() {
			found = true
		}
	}
	if !found {
		s.cleans = append(s.cleans, req.GetKey())
	}

	h := sha256.New()
	h.Write(req.GetValue().Value)
	hash := fmt.Sprintf("%x", h.Sum(nil))

	ts := time.Now().Unix()
	err := s.writeToDir(ctx, req.GetKey(), hash, &pb.ReadResponse{
		Hash:      hash,
		Value:     req.GetValue(),
		Timestamp: ts,
	}, "latest")
	if err != nil {
		s.writeLock.Unlock()
		return nil, err
	}

	s.writeLock.Unlock()
	count := 1
	if !req.NoFanout {
		friends, err := s.FFind(ctx, "dstore")
		if err == nil {
			req.NoFanout = true
			for _, friend := range friends {
				if !strings.HasPrefix(friend, s.Registry.GetIdentifier()) {
					conn, err := s.FDial(friend)
					if err == nil {
						client := pb.NewDStoreServiceClient(conn)
						t2 := time.Now()
						s.CtxLog(ctx, fmt.Sprintf("Writing %v as sub to %v", req.GetKey(), friend))
						_, err := client.Write(ctx, req)
						subLatency.With(prometheus.Labels{"method": "WRITE", "client": friend}).Observe(float64(time.Since(t2).Milliseconds()))
						if err == nil {
							count++
						}
						conn.Close()
					}
				}
			}
		}

		write_consensus.With(prometheus.Labels{"key": req.GetKey()}).Set(float64(float32(count) / float32(len(friends))))

		s.CtxLog(ctx, fmt.Sprintf("Written %v in %v", req.GetKey(), time.Since(t1)))
		return &pb.WriteResponse{
			Consensus: float32(count) / float32(len(friends)),
			Hash:      hash,
			Timestamp: ts,
		}, nil
	}

	return &pb.WriteResponse{}, nil
}
