package postgres

import (
	"context"
	"log"
	"order-service/config"
	pb "order-service/genproto/payment"
	"testing"
)

func paymentDB() *PaymentRepo {
	db, err := ConnectDB(config.Load())
	if err != nil {
		log.Fatal("could not connect to postgres")
	}

	return NewPaymentRepo(db)
}

func TestCreatePayment(t *testing.T) {
	p := paymentDB()

	payment := pb.NewPayment{
		OrderId:       "423e4567-e89b-12d3-a456-426614175007",
		PaymentMethod: "card",
		CardNumber:    "1234567891234567",
		ExpiryDate:    "01/01",
		Cvv:           "123",
	}
	_, err := p.Create(context.Background(), &payment)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
}

func TestReadPayment(t *testing.T) {
	p := paymentDB()

	exp := &pb.PaymentDetails{
		Id:            "623e4567-e89b-12d3-a456-426614177007",
		OrderId:       "423e4567-e89b-12d3-a456-426614175007",
		Amount:        94.49,
		Status:        "completed",
		Method:        "online",
		CardNumber:    "",
		ExpiryDate:    "",
		Cvv:           "",
		TransactionId: "ONLINE111222333",
	}
	res, err := p.Read(context.Background(), &pb.ID{Id: "623e4567-e89b-12d3-a456-426614177007"})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if res.Id != exp.Id || res.OrderId != exp.OrderId || res.Amount != exp.Amount || res.Status != exp.Status ||
		res.Method != exp.Method || res.CardNumber != exp.CardNumber || res.ExpiryDate != exp.ExpiryDate ||
		res.Cvv != exp.Cvv || res.TransactionId != exp.TransactionId {
		t.Errorf("Want %v, got %v", exp, res)
	}
}
