package postgres

import (
	"context"
	"log"
	"order-service/config"
	pb "order-service/genproto/order"
	"testing"
)

func orderDB() *OrderRepo {
	db, err := ConnectDB(&config.Config{
		DB_HOST:     "localhost",
		DB_PORT:     "5432",
		DB_USER:     "postgres",
		DB_NAME:     "local_eats_order",
		DB_PASSWORD: "root",
	})
	if err != nil {
		log.Fatal("could not connect to postgres")
	}

	return NewOrderRepo(db)
}

func TestMakeOrder(t *testing.T) {
	o := orderDB()

	it := &pb.Item{
		DishId:   "323e4567-e89b-12d3-a456-426614174110",
		Quantity: 2,
	}
	var data = &pb.NewOrder{
		UserId:          "123e4567-e89b-12d3-a456-426614174001",
		KitchenId:       "223e4567-e89b-12d3-a456-426614174001",
		Items:           []*pb.Item{it},
		DeliveryAddress: "Tashkent, Uzbekistan",
		DeliveryTime:    "2024-07-16 20:30:00",
	}

	resp, err := o.MakeOrder(context.Background(), data)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}

func TestReadOrder(t *testing.T) {
	o := orderDB()

	resp, err := o.Read(context.Background(), &pb.ID{Id: "423e4567-e89b-12d3-a456-426614175005"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}

func TestUpdateOrder(t *testing.T) {
	o := orderDB()

	resp, err := o.ChangeStatus(context.Background(), &pb.Status{
		Id:     "423e4567-e89b-12d3-a456-426614175001",
		Status: "delivered",
	})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}

func TestFetchForCustomer(t *testing.T) {
	o := orderDB()

	resp, err := o.FetchForCustomer(context.Background(), &pb.Pagination{
		Limit:  2,
		Offset: 0,
	})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}

func TestFetchForKitchen(t *testing.T) {
	o := orderDB()

	resp, err := o.FetchForKitchen(context.Background(), &pb.Filter{
		KitchenId: "223e4567-e89b-12d3-a456-426614174005",
		Status:    "pending",
		Pagination: &pb.Pagination{
			Limit:  2,
			Offset: 0,
		},
	})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}
