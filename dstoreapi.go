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
)

//Read reads out some data
func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	//Push the hash if we don't have one
	if req.GetHash() == "" {
		req.Hash = s.translate[req.GetKey()]
	}

	dir, file := extractFilename(req.GetKey())
	resp, err := s.readFile(dir, file, req.GetHash())

	code := status.Convert(err)
	if code.Code() != codes.OK && code.Code() != codes.NotFound {
		return nil, err
	}
	hashMap := make(map[string]*pb.ReadResponse)
	hashMap[resp.GetHash()] = resp
	countMap := make(map[string]int)
	countMap[resp.GetHash()] = 1
	bestCount := 1
	bestHash := resp.GetHash()

	req.NoFanout = true
	friends, err := s.FFind(ctx, "dstore")
	if err == nil {
		for _, friend := range friends {
			conn, err := s.FDialSpecificServer(ctx, "dstore", friend)
			if err == nil {
				client := pb.NewDStoreServiceClient(conn)

				read, err := client.Read(ctx, req)
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

	//Let's get a consensus on the latest
	retResp := hashMap[bestHash]
	s.Log(fmt.Sprintf("HERE HERE %v -> %v", bestHash, friends))
	s.Log(fmt.Sprintf("FRIENDS HERE %v, %v, %v", retResp, bestCount, len(friends)))
	retResp.Consensus = float32(bestCount) / float32(len(friends))

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
				if !strings.Contains(friend, s.Registry.Identifier) {
					conn, err := s.FDialSpecificServer(ctx, "dtore", friend)
					if err == nil {
						client := pb.NewDStoreServiceClient(conn)
						client.Write(ctx, req)
					}
				}
			}
		}
		return &pb.WriteResponse{Consensus: float32(count) / float32(len(friends))}, nil
	}

	return &pb.WriteResponse{}, nil
}
