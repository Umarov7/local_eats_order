package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	pb "order-service/genproto/order"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

func ConnectDB() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		log.Fatalf("could not connect to redis: %v", err)
	}

	return rdb
}

func PlaceOrder(ctx context.Context, ord *pb.NewOrder) error {
	rdb := ConnectDB()

	ordJson, err := json.Marshal(ord)
	if err != nil {
		return errors.Wrap(err, "order marshalling failure")
	}

	err = rdb.RPush(ctx, fmt.Sprintf("kitchen_orders:%s", ord.KitchenId), ordJson).Err()
	if err != nil {
		return errors.Wrap(err, "kitchen orders push failure")
	}

	err = rdb.RPush(ctx, fmt.Sprintf("user_orders:%s", ord.UserId), ordJson).Err()
	if err != nil {
		return errors.Wrap(err, "user orders push failure")
	}

	return nil
}
