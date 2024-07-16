package service

import (
	"context"
	"database/sql"
	"log/slog"
	pbk "order-service/genproto/kitchen"
	pb "order-service/genproto/order"
	pbu "order-service/genproto/user"
	"order-service/pkg/logger"
	"order-service/storage/postgres"

	"github.com/pkg/errors"
)

type OrderService struct {
	pb.UnimplementedOrderServer
	Repo          *postgres.OrderRepo
	UserClient    pbu.UserClient
	KitchenClient pbk.KitchenClient
	Logger        *slog.Logger
}

func NewOrderService(db *sql.DB, userCl pbu.UserClient, kitCl pbk.KitchenClient) *OrderService {
	return &OrderService{
		Repo:          postgres.NewOrderRepo(db),
		UserClient:    userCl,
		KitchenClient: kitCl,
		Logger:        logger.NewLogger(),
	}
}

func (s *OrderService) MakeOrder(ctx context.Context, req *pb.NewOrder) (*pb.NewOrderResp, error) {
	s.Logger.Info("Making order")

	resp, err := s.Repo.MakeOrder(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to make order")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Order made")
	return resp, nil
}

func (s *OrderService) ChangeStatus(ctx context.Context, req *pb.Status) (*pb.UpdatedOrder, error) {
	s.Logger.Info("Changing order status")

	resp, err := s.Repo.ChangeStatus(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to change order status")
		s.Logger.Error(er.Error())
		return nil, er
	}

	s.Logger.Info("Order status changed")
	return resp, nil
}

func (s *OrderService) GetOrderByID(ctx context.Context, req *pb.ID) (*pb.OrderInfo, error) {
	s.Logger.Info("Getting order")

	resp, err := s.Repo.Read(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to get order")
		s.Logger.Error(er.Error())
		return nil, er
	}

	kitchen, err := s.KitchenClient.Get(ctx, &pbk.ID{Id: resp.KitchenName})
	if err != nil {
		er := errors.Wrap(err, "failed to get kitchen name")
		s.Logger.Error(er.Error())
		return nil, er
	}
	resp.KitchenName = kitchen.Name

	s.Logger.Info("Order fetched")
	return resp, nil
}

func (s *OrderService) FetchOrdersForCustomer(ctx context.Context, req *pb.Pagination) (*pb.OrdersCustomer, error) {
	s.Logger.Info("Fetching orders for customer")

	resp, err := s.Repo.FetchForCustomer(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to fetch orders for customer")
		s.Logger.Error(er.Error())
		return nil, er
	}

	for _, v := range resp {
		kn, err := s.KitchenClient.Get(ctx, &pbk.ID{Id: v.KitchenName})
		if err != nil {
			er := errors.Wrap(err, "failed to get kitchen name")
			s.Logger.Error(er.Error())
			return nil, er
		}
		v.KitchenName = kn.Name
	}

	s.Logger.Info("Orders fetched")
	return &pb.OrdersCustomer{
		Orders: resp,
		Total:  int32(len(resp)),
		Page:   req.Offset / req.Limit,
		Limit:  req.Limit,
	}, nil
}

func (s *OrderService) FetchOrdersForKitchen(ctx context.Context, req *pb.Filter) (*pb.OrdersKitchen, error) {
	s.Logger.Info("Fetching orders for kitchen")

	resp, err := s.Repo.FetchForKitchen(ctx, req)
	if err != nil {
		er := errors.Wrap(err, "failed to fetch orders for kitchen")
		s.Logger.Error(er.Error())
		return nil, er
	}

	for _, v := range resp {
		user, err := s.UserClient.GetProfile(ctx, &pbu.ID{Id: v.UserName})
		if err != nil {
			er := errors.Wrap(err, "failed to get username")
			s.Logger.Error(er.Error())
			return nil, er
		}
		v.UserName = user.Username
	}

	s.Logger.Info("Orders fetched")
	return &pb.OrdersKitchen{
		Orders: resp,
		Total:  int32(len(resp)),
		Page:   req.Pagination.Offset / req.Pagination.Limit,
		Limit:  req.Pagination.Limit,
	}, nil
}
