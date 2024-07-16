package pkg

import (
	"errors"
	"log"
	"order-service/config"
	pbk "order-service/genproto/kitchen"
	pbu "order-service/genproto/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func CreateUserClient(cfg *config.Config) pbu.UserClient {
	conn, err := grpc.NewClient(cfg.AUTH_PORT,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Println(errors.New("failed to connect to the address: " + err.Error()))
		return nil
	}

	return pbu.NewUserClient(conn)
}

func CreateKitchenClient(cfg *config.Config) pbk.KitchenClient {
	conn, err := grpc.NewClient(cfg.AUTH_PORT,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Println(errors.New("failed to connect to the address: " + err.Error()))
		return nil
	}

	return pbk.NewKitchenClient(conn)
}
