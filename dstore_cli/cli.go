package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pb "github.com/brotherlogic/dstore/proto"
	google_protobuf "github.com/golang/protobuf/ptypes/any"

	//Needed to pull in gzip encoding init
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(&utils.DiscoveryClientResolverBuilder{})
}

func main() {
	all, err := utils.BaseResolveAll("dstore")
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}

	//rand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	chosen := all[0]
	fmt.Printf("Writing to %v\n", chosen)
	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", chosen.Identifier, chosen.Port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewDStoreServiceClient(conn)
	ctx, cancel := utils.ManualContext("dstore-cli", time.Minute)
	defer cancel()

	switch os.Args[1] {
	case "write":
		res, err := client.Write(ctx, &pb.WriteRequest{Key: os.Args[2], Value: &google_protobuf.Any{Value: []byte(os.Args[3])}})
		fmt.Printf("%v -> %v\n", res, err)
	case "read":
		res, err := client.Read(ctx, &pb.ReadRequest{Key: os.Args[2], NoFanout: true})
		fmt.Printf("%v -> %v\n", res, err)
	}

}
