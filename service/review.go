package service

import (
	"context"
	"database/sql"
	"log/slog"
	pbk "order-service/genproto/kitchen"
	pb "order-service/genproto/review"
	pbu "order-service/genproto/user"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type ReviewService struct {
	pb.UnimplementedReviewServer
	Repo          *postgres.ReviewRepo
	OrderRepo     *postgres.OrderRepo
	UserClient    pbu.UserClient
	KitchenClient pbk.KitchenClient
	Logger        *slog.Logger
}

func NewReviewService(db *sql.DB, userCl pbu.UserClient, knCl pbk.KitchenClient) *ReviewService {
	return &ReviewService{
		Repo:          postgres.NewReviewRepo(db),
		OrderRepo:     postgres.NewOrderRepo(db),
		UserClient:    userCl,
		KitchenClient: knCl,
		Logger:        logger.NewLogger(),
	}
}

func (s *ReviewService) RateAndComment(ctx context.Context, req *pb.NewReview) (*pb.NewReviewResp, error) {
	s.Logger.Info("Creating review")

	status, err := s.OrderRepo.GetStatus(ctx, req.OrderId)
	if err != nil {
		er := errors.Wrap(err, "failed to get order status")
		s.Logger.Error(er.Error())
		return nil, er
	}
	if status != "completed" {
		er := errors.New("order is not completed")
		s.Logger.Error(er.Error())
		return nil, er
	}

	resp, err := s.Repo.Create(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to create review")
		s.Logger.Error(er.Error())
		return nil, er
	}

	_, kitchenID, err := s.OrderRepo.GetIDs(ctx, req.OrderId)
	if err != nil {
		er := errors.Wrap(err, "failed to get kitchen id")
		s.Logger.Error(er.Error())
		return nil, er
	}

	_, err = s.KitchenClient.UpdateRating(ctx, &pbk.Rating{Id: kitchenID, Rating: req.Rating})
	if err != nil {
		er := errors.Wrap(err, "failed to update rating")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Review created")
	return resp, nil
}

func (s *ReviewService) GetReviewOfKitchen(ctx context.Context, req *pb.Filter) (*pb.Reviews, error) {
	s.Logger.Info("Getting reviews of kitchen")

	resp, err := s.Repo.GetKitchenReviews(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to get reviews of kitchen")
		s.Logger.Error(er.Error())
		return nil, er
	}

	for _, v := range resp.Reviews {
		user, err := s.UserClient.GetProfile(ctx, &pbu.ID{Id: v.UserName})
		if err != nil {
			er := errors.Wrap(err, "failed to get username")
			s.Logger.Error(er.Error())
			return nil, er
		}
		v.UserName = user.Username
	}

	s.Logger.Info("Reviews of kitchen received")
	return resp, nil
}
