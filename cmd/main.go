package main

import (
	"log"
	"net"
	"order-service/config"
	pbd "order-service/genproto/dish"
	pbe "order-service/genproto/extra"
	pbo "order-service/genproto/order"
	pbp "order-service/genproto/payment"
	pbr "order-service/genproto/review"
	"order-service/pkg"
	"order-service/service"
	"order-service/storage/postgres"

	"google.golang.org/grpc"
)

func main() {
	cfg := config.Load()

	db, err := postgres.ConnectDB(cfg)
	if err != nil {
		log.Fatalf("error while connecting to database: %v", err)
	}
	defer db.Close()

	lis, err := net.Listen("tcp", cfg.ORDER_PORT)
	if err != nil {
		log.Fatalf("error while listening: %v", err)
	}
	defer lis.Close()

	userClient := pkg.CreateUserClient(cfg)
	kitchenClient := pkg.CreateKitchenClient(cfg)

	dishService := service.NewDishService(db, kitchenClient)
	orderService := service.NewOrderService(db, userClient, kitchenClient, cfg)
	reviewService := service.NewReviewService(db, userClient, kitchenClient)
	paymentService := service.NewPaymentService(db, kitchenClient)
	extraService := service.NewExtraService(db, kitchenClient)

	server := grpc.NewServer()
	pbd.RegisterDishServer(server, dishService)
	pbo.RegisterOrderServer(server, orderService)
	pbr.RegisterReviewServer(server, reviewService)
	pbp.RegisterPaymentServer(server, paymentService)
	pbe.RegisterExtraServer(server, extraService)

	log.Printf("server listening at %v", lis.Addr())
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("error while serving: %v", err)
	}
}
