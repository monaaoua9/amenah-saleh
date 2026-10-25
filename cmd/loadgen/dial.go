package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func grpcInsecure() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}
