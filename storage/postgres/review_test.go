package postgres

import (
	"context"
	"log"
	"order-service/config"
	pb "order-service/genproto/review"
	"testing"
)

func reviewDB() *ReviewRepo {
	db, err := ConnectDB(config.Load())
	if err != nil {
		log.Fatal("could not connect to postgres")
	}

	return NewReviewRepo(db)
}

func TestCreateReview(t *testing.T) {
	r := reviewDB()

	rev := &pb.NewReview{
		OrderId: "423e4567-e89b-12d3-a456-426614175007",
		Rating:  3.5,
		Comment: "test comment",
	}

	res, err := r.Create(context.Background(), rev)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if res.Id == "" || res.OrderId != rev.OrderId || res.Rating != rev.Rating || res.Comment != rev.Comment {
		t.Errorf("Want %v, got %v", rev, res)
	}
}

func TestGetKitchenReviews(t *testing.T) {
	filter := &pb.Filter{
		KitchenId: "223e4567-e89b-12d3-a456-426614174006",
		Limit:     2,
		Offset:    0,
	}

	resp, err := reviewDB().GetKitchenReviews(context.Background(), filter)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp == nil {
		t.Errorf("Want non-nil, got %v", resp)
	}
}
