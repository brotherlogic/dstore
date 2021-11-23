package main

import (
	"fmt"
	"time"

	"github.com/brotherlogic/goserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/brotherlogic/dstore/proto"
	pbg "github.com/brotherlogic/goserver/proto"
	"github.com/brotherlogic/goserver/utils"
)

//Server main server type
type Server struct {
	*goserver.GoServer
	basepath  string
	translate map[string]string
	cleans    []string
}

// Init builds the server
func Init() *Server {
	s := &Server{
		GoServer:  &goserver.GoServer{},
		basepath:  "/media/datastore/",
		translate: make(map[string]string),
		cleans:    make([]string, 0),
	}
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pb.RegisterDStoreServiceServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*pbg.State {
	return []*pbg.State{}
}

func (s *Server) runCleans() {
	for !s.LameDuck {
		time.Sleep(time.Minute)
		s.Log(fmt.Sprintf("Starting clean: %v", len(s.cleans)))
		for _, key := range s.cleans {
			ctx, cancel := utils.ManualContext("dstore-clean-"+key, time.Minute)
			err := s.cleanDir(ctx, key)
			if err != nil {
				s.RaiseIssue("Bad clean", fmt.Sprintf("Cleaning failure: %v", err))
			}
			cancel()
		}
	}
}

func main() {
	server := Init()
	server.PrepServer()
	server.Register = server

	err := server.RegisterServerV2("dstore", false, true)
	if err != nil {
		return
	}

	server.MemCap = 100000000 * 100
	go server.runCleans()

	fmt.Printf("%v", server.Serve())
}
