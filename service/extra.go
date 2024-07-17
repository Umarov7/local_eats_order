package service

import (
	"context"
	"database/sql"
	"log/slog"
	pb "order-service/genproto/extra"
	pbk "order-service/genproto/kitchen"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type ExtraService struct {
	pb.UnimplementedExtraServer
	Repo      *postgres.BonusRepo
	KitchenCl pbk.KitchenClient
	Logger    *slog.Logger
}

func NewExtraService(db *sql.DB, knCl pbk.KitchenClient) *ExtraService {
	return &ExtraService{
		Repo:      postgres.NewBonusRepo(db),
		KitchenCl: knCl,
		Logger:    logger.NewLogger(),
	}
}

func (s *ExtraService) GetStatistics(ctx context.Context, req *pb.Period) (*pb.Statistics, error) {
	s.Logger.Info("Getting kitchen statistics")

	resp, err := s.Repo.GetKitchenStatistics(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to get kitchen statistics")
		s.Logger.Error(er.Error())
		return nil, er
	}

	return resp, nil
}

func (s *ExtraService) TrackActivity(ctx context.Context, req *pb.Period) (*pb.Activity, error) {
	s.Logger.Info("Tracking user activity")

	resp, err := s.Repo.TrackActivity(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to track user activity")
		s.Logger.Error(er.Error())
		return nil, er
	}

	for _, v := range resp.FavoriteKitchens {
		name, err := s.KitchenCl.GetName(ctx, &pbk.ID{Id: v.Id})
		if err != nil {
			er := errors.Wrap(err, "failed to get kitchen name")
			s.Logger.Error(er.Error())
			return nil, er
		}
		v.Name = name.Name
	}

	return resp, nil
}

func (s *ExtraService) SetWorkingHours(ctx context.Context, req *pb.WorkingHours) (*pb.WorkingHoursResp, error) {
	s.Logger.Info("Setting kitchen working hours")

	resp, err := s.Repo.SetWorkingHours(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to set working hours")
		s.Logger.Error(er.Error())
		return nil, er
	}

	return resp, nil
}

func (s *ExtraService) GetNutrition(ctx context.Context, req *pb.ID) (*pb.NutritionalInfo, error) {
	s.Logger.Info("Getting dish's nutrition info")

	resp, err := s.Repo.GetNutrition(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to get dish's nutrition info")
		s.Logger.Error(er.Error())
		return nil, er
	}

	return resp, nil
}
