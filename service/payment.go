package service

import (
	"context"
	"database/sql"
	"log/slog"
	pb "order-service/genproto/payment"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type PaymentService struct {
	pb.UnimplementedPaymentServer
	Repo   *postgres.PaymentRepo
	Logger *slog.Logger
}

func NewPaymentService(db *sql.DB) *PaymentService {
	return &PaymentService{
		Repo:   postgres.NewPaymentRepo(db),
		Logger: logger.NewLogger(),
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
