package postgres

import (
	"context"
	"log"
	"order-service/config"
	pb "order-service/genproto/dish"
	"reflect"
	"testing"
)

func dishDB() *DishRepo {
	db, err := ConnectDB(config.Load())
	if err != nil {
		log.Fatal("could not connect to postgres")
	}

	return NewDishRepo(db)
}

func TestCreate(t *testing.T) {
	d := dishDB()

	newDish := &pb.NewDish{
		KitchenId:   "223e4567-e89b-12d3-a456-426614174001",
		Name:        "Test Dish",
		Description: "This is a test dish",
		Price:       9.99,
		Category:    "Main Course",
		Ingredients: []string{"ingredient1", "ingredient2"},
		Available:   true,
	}

	resp, err := d.Create(context.Background(), newDish)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}

func TestRead(t *testing.T) {
	d := dishDB()

	dishInfo, err := d.Read(context.Background(), &pb.ID{Id: "323e4567-e89b-12d3-a456-426614174102"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if dishInfo == nil {
		t.Errorf("Want non-nil, got %v", dishInfo)
	}
}

func TestUpdate(t *testing.T) {
	d := dishDB()

	newData := &pb.NewData{
		Id:        "323e4567-e89b-12d3-a456-426614174102",
		Name:      "Test Dish",
		Price:     9.99,
		Available: true,
	}

	resp, err := d.Update(context.Background(), newData)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}

func TestDelete(t *testing.T) {
	d := dishDB()

	err := d.Delete(context.Background(), &pb.ID{Id: "323e4567-e89b-12d3-a456-426614174101"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

func TestFetch(t *testing.T) {
	d := dishDB()

	f := &pb.Pagination{
		Limit:  2,
		Offset: 0,
	}

	exp := []*pb.DishDetails{
		{
			Id:        "323e4567-e89b-12d3-a456-426614174103",
			Name:      "Chocolate Fudge Cake",
			Price:     8.75,
			Category:  "Dessert",
			Available: true,
		},
		{
			Id:        "323e4567-e89b-12d3-a456-426614174104",
			Name:      "Red Velvet Cupcake",
			Price:     3.99,
			Category:  "Dessert",
			Available: true,
		},
	}

	res, err := d.Fetch(context.Background(), f)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if res == nil {
		t.Errorf("Want non-nil, got %v", res)
	}

	if !reflect.DeepEqual(exp, res) {
		t.Errorf("Want %v, got %v", exp, res)
	}
}
