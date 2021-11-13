package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/brotherlogic/dstore/proto"
)

func (s *Server) deleteFile(dir, file string) {
	s.Log(fmt.Sprintf("Removing %v%v -> %v", dir, file, os.Remove(s.basepath+dir+file)))
}

func (s *Server) readFile(key, hash string) (*pb.ReadResponse, error) {
	data, err := ioutil.ReadFile(s.basepath + key + "/" + hash)

	if err != nil {
		if os.IsNotExist(err) {
			s.Log(fmt.Sprintf("Cannot fine %v", s.basepath+key+"/"+hash))
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

func (s *Server) writeToDir(dir, file string, toWrite *pb.ReadResponse, lnfile string) error {

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
		err2 := os.Remove(fmt.Sprintf("%v%v/%v", s.basepath, dir, lnfile))
		if err2 != nil {
			s.Log(fmt.Sprintf("Unable to remove the latest: %v", err2))
		}
		err = os.Symlink(fmt.Sprintf("%v", file), fmt.Sprintf("%v%v/%v", s.basepath, dir, lnfile))
	}

	return err
}
