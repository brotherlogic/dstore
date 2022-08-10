package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"

	dspb "github.com/brotherlogic/datastore/proto"
	pb "github.com/brotherlogic/dstore/proto"
	rcpb "github.com/brotherlogic/recordcleaner/proto"
	google_protobuf "github.com/golang/protobuf/ptypes/any"

	//Needed to pull in gzip encoding init
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
	"google.golang.org/protobuf/proto"
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
	chosen := all[1]
	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", chosen.Identifier, chosen.Port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewDStoreServiceClient(conn)
	ctx, cancel := utils.ManualContext("dstore-cli", time.Minute)
	defer cancel()

	switch os.Args[1] {
	case "latest":
		key := ""
		if len(os.Args) > 3 {
			key = os.Args[3]
		}
		for _, server := range all {
			conn, err := utils.LFDial(fmt.Sprintf("%v:%v", server.Identifier, server.Port))
			if err != nil {
				log.Fatalf("Unable to dial %v -> %v", server, err)
			}
			defer conn.Close()
			client := pb.NewDStoreServiceClient(conn)
			res, err := client.GetLatest(ctx, &pb.GetLatestRequest{Key: os.Args[2], Hash: key})
			if err != nil {
				log.Fatalf("Unable to read %v -> %v", server, err)
			}
			fmt.Printf("%v: %v\n", server.Identifier, res.GetHash())
		}
	case "write":
		fmt.Printf("Writing to %v\n", chosen)

		res, err := client.Write(ctx, &pb.WriteRequest{Key: os.Args[2], Value: &google_protobuf.Any{Value: []byte(os.Args[3])}})
		fmt.Printf("%v -> %v\n", res, err)
	case "read":
		res, err := client.Read(ctx, &pb.ReadRequest{Key: os.Args[2]})
		fmt.Printf("%v -> %v\n", res, err)

		config := &rcpb.Config{}
		err = proto.Unmarshal(res.GetValue().GetValue(), config)
		if err != nil {
			log.Fatalf("ERR: %v", err)
		}
		log.Printf("CONFIG: %v", config)
	case "transfer":
		conn, err := utils.LFDialServer(ctx, "datastore")
		if err != nil {
			log.Fatalf("Error on dial %v", err)
		}
		defer conn.Close()
		dataclient := dspb.NewDatastoreServiceClient(conn)

		read, err := dataclient.Read(ctx, &dspb.ReadRequest{Key: os.Args[2]})
		if err != nil {
			log.Fatalf("Error on read: %v", err)
		}

		res, err := client.Write(ctx, &pb.WriteRequest{Key: os.Args[3], Value: read.GetValue()})
		log.Printf("Written: %v and %v", res, err)
	}

}
