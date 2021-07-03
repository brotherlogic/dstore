package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/brotherlogic/dstore/proto"
)

func extractFilename(key string) (string, string) {
	val := strings.LastIndex(key, "/")
	if val < 0 {
		return "", key
	}
	return key[0 : val+1], key[val+1:]
}

func (s *Server) deleteFile(dir, file string) {
	s.Log(fmt.Sprintf("Removing %v%v -> %v", dir, file, os.Remove(s.basepath+dir+file)))
}

func (s *Server) readFile(dir, key, hash string) (*pb.ReadResponse, error) {
	data, err := ioutil.ReadFile(s.basepath + key + "/" + hash)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, err.Error())
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
	}

	if len(lnfile) > 0 && err == nil {
		//Silent delete of existing symlink
		os.Remove(fmt.Sprintf("%v%v/%v", s.basepath, dir, lnfile))
		err = os.Symlink(fmt.Sprintf("%v", file), fmt.Sprintf("%v%v/%v", s.basepath, dir, lnfile))
	}

	return err
}
