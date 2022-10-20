package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/brotherlogic/dstore/proto"
)

func (s *Server) deleteFile(dir, file string) {
	//s.Log(fmt.Sprintf("Removing %v%v -> %v", dir, file, os.Remove(s.basepath+dir+file)))
}

func (s *Server) readFile(key, hash string) (*pb.ReadResponse, error) {
	data, err := ioutil.ReadFile(s.basepath + key + "/" + hash)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		return nil, err
	}

	result := &pb.ReadResponse{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Server) writeToDir(ctx context.Context, dir, file string, toWrite *pb.ReadResponse, lnfile string) error {

	filepath := fmt.Sprintf("%v%v/%v", s.basepath, dir, file)

	// Don't write if the file exists
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		data, err := proto.Marshal(toWrite)
		if err != nil {
			return err
		}

		os.MkdirAll(s.basepath+dir, 0777)
		err = ioutil.WriteFile(filepath, data, 0644)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// We need to reset the err here
	err = nil
	if len(lnfile) > 0 {
		//Silent delete of existing symlink
		os.Remove(fmt.Sprintf("%v%v/%v", s.basepath, dir, lnfile))
		err = os.Symlink(fmt.Sprintf("%v", file), fmt.Sprintf("%v%v/%v", s.basepath, dir, lnfile))
	}

	return err
}

var (
	key_size = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dstore_key_size",
		Help: "The oldest physical record",
	}, []string{"key"})
)

func (s *Server) cleanDir(ctx context.Context, key string) error {
	files, err := ioutil.ReadDir(s.basepath + key)

	if err != nil {
		return err
	}

	key_size.With(prometheus.Labels{"key": key}).Set(float64(len(files)))
	if len(files) > 2000 {
		sort.SliceStable(files, func(i, j int) bool {
			return files[i].ModTime().Before(files[j].ModTime())
		})

		for i := 0; i < len(files)-1000; i++ {
			err = os.Remove(s.basepath + key + "/" + files[i].Name())
		}
	}

	return nil
}
