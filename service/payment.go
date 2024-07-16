package service

import (
	"context"
	"database/sql"
	"log/slog"
	pbk "order-service/genproto/kitchen"
	pb "order-service/genproto/payment"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type PaymentService struct {
	pb.UnimplementedPaymentServer
	Repo          *postgres.PaymentRepo
	OrderRepo     *postgres.OrderRepo
	KitchenClient pbk.KitchenClient
	Logger        *slog.Logger
}

func NewPaymentService(db *sql.DB, kitchenCl pbk.KitchenClient) *PaymentService {
	return &PaymentService{
		Repo:          postgres.NewPaymentRepo(db),
		OrderRepo:     postgres.NewOrderRepo(db),
		KitchenClient: kitchenCl,
		Logger:        logger.NewLogger(),
	}
}

func (s *PaymentService) MakePayment(ctx context.Context, req *pb.NewPayment) (*pb.NewPaymentResp, error) {
	s.Logger.Info("Making payment")

	resp, err := s.Repo.Create(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to make payment")
		s.Logger.Error(er.Error())
		return nil, er
	}

	kitchenID, err := s.OrderRepo.GetKitchenID(ctx, req.OrderId)
	if err != nil {
		er := errors.Wrap(err, "failed to get kitchen id")
		s.Logger.Error(er.Error())
		return nil, er
	}

	_, err = s.KitchenClient.IncrementTotalOrders(ctx, &pbk.ID{Id: kitchenID})
	if err != nil {
		er := errors.Wrap(err, "failed to increment total orders")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Payment made")
	return resp, nil
}

func (s *PaymentService) GetPayment(ctx context.Context, req *pb.ID) (*pb.PaymentDetails, error) {
	s.Logger.Info("Getting payment")

	resp, err := s.Repo.Read(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to get payment")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Payment gotten")
	return resp, nil
}
