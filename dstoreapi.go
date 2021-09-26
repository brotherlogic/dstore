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
)

//Read reads out some data
func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
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

	if !req.NoFanout {
		req.NoFanout = true
		friends, err = s.FFind(ctx, "dstore")
		if err == nil {
			for _, friend := range friends {
				if !strings.HasPrefix(friend, s.Registry.GetIdentifier()) {
					conn, err := s.FDial(friend)
					if err == nil {
						client := pb.NewDStoreServiceClient(conn)

						read, err := client.Read(ctx, req)
						s.Log(fmt.Sprintf("I'VE READ %v FROM %v -> %v", req.GetKey(), friend, err))
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
		return nil, status.Errorf(codes.NotFound, "Cannot locate %v", req.GetKey())
	}

	//Let's get a consensus on the latest
	retResp := hashMap[bestHash]
	s.Log(fmt.Sprintf("MAPP %v -> %v", req.GetKey(), hashMap))
	s.Log(fmt.Sprintf("READ %v with %v [%v]", bestCount, friends, bestHash))
	retResp.Consensus = float32(bestCount) / float32(len(friends))

	read_consensus.With(prometheus.Labels{"key": req.GetKey()}).Set(float64(retResp.GetConsensus()))

	return retResp, nil
}

//Write writes out a key
func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if strings.HasPrefix(req.GetKey(), "/") {
		return nil, fmt.Errorf("keys should not start with a backslash: %v", req.GetKey())
	}

	h := sha256.New()
	h.Write(req.GetValue().Value)
	hash := fmt.Sprintf("%x", h.Sum(nil))

	err := s.writeToDir(req.GetKey(), hash, &pb.ReadResponse{
		Hash:      hash,
		Value:     req.GetValue(),
		Timestamp: time.Now().Unix(),
	}, "latest")
	if err != nil {
		return nil, err
	}

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
						_, err := client.Write(ctx, req)
						s.Log(fmt.Sprintf("I'VE READ FROM %v -> %v", friend, err))
						if err != nil {
							s.Log(fmt.Sprintf("Fanout failure: %v", err))
						} else {
							count++
						}
						conn.Close()
					} else {
						s.Log(fmt.Sprintf("WHAT: %v", err))
					}
				}
			}
		}
		s.Log(fmt.Sprintf("Written to %v with %v", count, friends))

		write_consensus.With(prometheus.Labels{"key": req.GetKey()}).Set(float64(float32(count) / float32(len(friends))))

		return &pb.WriteResponse{Consensus: float32(count) / float32(len(friends))}, nil
	}

	return &pb.WriteResponse{}, nil
}
