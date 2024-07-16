package service

import (
	"context"
	"database/sql"
	"log/slog"
	pb "order-service/genproto/dish"
	pbk "order-service/genproto/kitchen"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type DishService struct {
	pb.UnimplementedDishServer
	Repo          *postgres.DishRepo
	KitchenClient pbk.KitchenClient
	Logger        *slog.Logger
}

func NewDishService(db *sql.DB, kitchenCl pbk.KitchenClient) *DishService {
	return &DishService{
		Repo:          postgres.NewDishRepo(db),
		KitchenClient: kitchenCl,
		Logger:        logger.NewLogger(),
	}
}

func (s *DishService) Add(ctx context.Context, req *pb.NewDish) (*pb.NewDishResp, error) {
	s.Logger.Info("Adding new dish")

	status, err := s.KitchenClient.ValidateKitchen(ctx, &pbk.ID{Id: req.KitchenId})
	if err != nil {
		s.Logger.Error(err.Error())
		return nil, err
	}
	if !status.Exists {
		s.Logger.Error("kitchen not found")
		return nil, errors.New("kitchen not found")
	}

	s.Logger.Info("Kitchen exists")

	resp, err := s.Repo.Create(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to add new dish")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("New dish added")
	return resp, nil
}

func (s *DishService) Read(ctx context.Context, req *pb.ID) (*pb.DishInfo, error) {
	s.Logger.Info("Reading dish")

	resp, err := s.Repo.Read(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to read dish")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Dish read")
	return resp, nil
}

func (s *DishService) Update(ctx context.Context, req *pb.NewData) (*pb.UpdatedData, error) {
	s.Logger.Info("Updating dish")

	resp, err := s.Repo.Update(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to update dish")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Dish updated")
	return resp, nil
}

func (s *DishService) Delete(ctx context.Context, req *pb.ID) (*pb.Void, error) {
	s.Logger.Info("Deleting dish")

	err := s.Repo.Delete(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to delete dish")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Dish deleted")
	return &pb.Void{}, nil
}

func (s *DishService) Fetch(ctx context.Context, req *pb.Pagination) (*pb.Dishes, error) {
	s.Logger.Info("Fetching dishes")

	resp, err := s.Repo.Fetch(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to fetch dishes")
		s.Logger.Error(er.Error())
		return nil, er
	}

	total, err := s.Repo.CountRows(ctx)
	if err != nil {
		er := errors.Wrap(err, "failed to count dishes")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Dishes fetched")
	return &pb.Dishes{
		Dishes: resp,
		Total:  int32(total),
		Page:   req.Offset / req.Limit,
		Limit:  req.Limit,
	}, nil
}
