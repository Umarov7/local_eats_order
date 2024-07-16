package service

import (
	"context"
	"database/sql"
	"log/slog"
	pb "order-service/genproto/review"
	pbu "order-service/genproto/user"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type ReviewService struct {
	pb.UnimplementedReviewServer
	Repo       *postgres.ReviewRepo
	UserClient pbu.UserClient
	Logger     *slog.Logger
}

func NewReviewService(db *sql.DB, userCl pbu.UserClient) *ReviewService {
	return &ReviewService{
		Repo:       postgres.NewReviewRepo(db),
		UserClient: userCl,
		Logger:     logger.NewLogger(),
	}
}

func (s *ReviewService) RateAndComment(ctx context.Context, req *pb.NewReview) (*pb.NewReviewResp, error) {
	s.Logger.Info("Creating review")

	resp, err := s.Repo.Create(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to create review")
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
