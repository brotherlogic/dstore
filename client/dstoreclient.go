package dstore_client

import (
	"context"

	pb "github.com/brotherlogic/dstore/proto"
	pbgs "github.com/brotherlogic/goserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type DStoreClient struct {
	Gs              *pbgs.GoServer
	ReadResponseMap map[string][]byte
	ErrorCode       map[string]codes.Code
	Test            bool
}

func (c *DStoreClient) Read(ctx context.Context, in *pb.ReadRequest, opts ...grpc.CallOption) (*pb.ReadResponse, error) {
	if c.Test {
		if val, ok := c.ErrorCode[in.GetKey()]; ok {
			return nil, status.Errorf(val, "Request error")
		}

		return &pb.ReadResponse{
			Value:     &anypb.Any{Value: c.ReadResponseMap[in.GetKey()]},
			Consensus: 1.0,
		}, nil
	}
	conn, err := c.Gs.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewDStoreServiceClient(conn)
	return client.Read(ctx, in, opts...)
}

func (c *DStoreClient) Write(ctx context.Context, in *pb.WriteRequest, opts ...grpc.CallOption) (*pb.WriteResponse, error) {
	if c.Test {
		if c.ReadResponseMap == nil {
			c.ReadResponseMap = make(map[string][]byte)
		}
		c.ReadResponseMap[in.GetKey()] = in.GetValue().GetValue()
		return &pb.WriteResponse{Consensus: 1.0}, nil
	}
	conn, err := c.Gs.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewDStoreServiceClient(conn)
	return client.Write(ctx, in, opts...)
}
