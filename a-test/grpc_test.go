package a_test

import (
	"context"
	"log"
	"strings"
	"testing"
	pb "toupper/proto"
)

// grpc服务端代码

const (
	port = ":50051"
)

type server struct{}

func (s *server) Upper(ctx context.Context, in *pb.UpperRequest) (*pb.UpperReply, error) {
	log.Printf("Received: %s", in.Name)

	return &pb.UpperReply{Message: strings.ToUpper(in.Name)}, nil
}

func GrpcRedis(t *testing.T) {

}
