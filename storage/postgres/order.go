package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	pb "order-service/genproto/order"

	"github.com/pkg/errors"
)

type OrderRepo struct {
	DB *sql.DB
}

func NewOrderRepo(db *sql.DB) *OrderRepo {
	return &OrderRepo{DB: db}
}

func (o *OrderRepo) MakeOrder(ctx context.Context, data *pb.NewOrder) (*pb.NewOrderResp, error) {
	query := `
	insert into
		orders (user_id, kitchen_id, items, total_amount, delivery_address, delivery_time)
	values
		($1, $2, $3, $4, $5, $6)
	returning
		id, user_id, kitchen_id, total_amount, delivery_address, delivery_time, created_at
	`
	var sum float32
	for _, v := range data.Items {
		price, err := o.GetPrice(ctx, v.DishId)
		if err != nil {
			return nil, err
		}
		sum += price * float32(v.Quantity)
	}

	var items []Item
	for _, v := range data.Items {
		items = append(items, Item{DishId: v.DishId, Quantity: int(v.Quantity)})
	}

	itemsBytes, err := json.Marshal(items)
	if err != nil {
		return nil, errors.Wrap(err, "items marshalling failure")
	}

	row := o.DB.QueryRowContext(ctx, query, data.UserId, data.KitchenId,
		itemsBytes, sum, data.DeliveryAddress, data.DeliveryTime,
	)

	var ord pb.NewOrderResp
	err = row.Scan(&ord.Id, &ord.UserId, &ord.KitchenId,
		&ord.TotalAmount, &ord.DeliveryAddress, &ord.DeliveryTime, &ord.CreatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(err, "insertion failure")
	}

	ord.Items = data.Items
	return &ord, nil
}

func (o *OrderRepo) Read(ctx context.Context, id *pb.ID) (*pb.OrderInfo, error) {
	query := `
	select
		user_id, kitchen_id, items, total_amount,
		status, delivery_address, delivery_time, created_at, updated_at
	from
		orders
	where
		deleted_at is null and id = $1
	`
	ord := pb.OrderInfo{Id: id.Id}

	var itemsByte []byte
	var items []*pb.Item
	err := o.DB.QueryRowContext(ctx, query, ord.Id).Scan(&ord.UserId, &ord.KitchenId,
		&itemsByte, &ord.TotalAmount, &ord.Status, &ord.DeliveryAddress,
		&ord.DeliveryTime, &ord.CreatedAt, &ord.UpdatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(err, "reading failure")
	}

	err = json.Unmarshal(itemsByte, &items)
	if err != nil {
		return nil, errors.Wrap(err, "items unmarshalling failure")
	}

	var itemDetails []*pb.ItemDetails
	for _, v := range items {
		name, err := o.GetName(ctx, v.DishId)
		if err != nil {
			return nil, errors.Wrap(err, "name retrieval failure")
		}

		price, err := o.GetPrice(ctx, v.DishId)
		if err != nil {
			return nil, errors.Wrap(err, "price retrieval failure")
		}

		itemDetails = append(itemDetails, &pb.ItemDetails{
			DishId:   v.DishId,
			Name:     name,
			Price:    price,
			Quantity: v.Quantity,
		})
	}

	ord.Items = itemDetails
	return &ord, nil
}

func (o *OrderRepo) ChangeStatus(ctx context.Context, sts *pb.Status) (*pb.UpdatedOrder, error) {
	query := `
	update
		orders
	set
		status = $1, updated_at = NOW()
	where
		deleted_at is null and id = $2
	returning
		id, status, updated_at
	`

	var upData pb.UpdatedOrder
	err := o.DB.QueryRowContext(ctx, query, sts.Status, sts.Id).Scan(
		&upData.Id, &upData.Status, &upData.UpdatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(err, "update failure")
	}

	return &upData, nil
}

func (o *OrderRepo) FetchForCustomer(ctx context.Context, pag *pb.Pagination) ([]*pb.OrderCustomer, error) {
	query := `
	select
		id, kitchen_id, total_amount, status, delivery_time
	from
		orders
	where
		deleted_at is null
	limit $1
	offset $2
	`

	rows, err := o.DB.QueryContext(ctx, query, pag.Limit, pag.Offset)
	if err != nil {
		return nil, errors.Wrap(err, "retrieval failure")
	}
	defer rows.Close()

	var orders []*pb.OrderCustomer
	for rows.Next() {
		var ord pb.OrderCustomer

		err := rows.Scan(&ord.Id, &ord.KitchenName, &ord.TotalAmount,
			&ord.Status, &ord.DeliveryTime,
		)
		if err != nil {
			return nil, errors.Wrap(err, "reading failure")
		}

		orders = append(orders, &ord)
	}

	return orders, nil
}

func (o *OrderRepo) FetchForKitchen(ctx context.Context, f *pb.Filter) ([]*pb.OrderKitchen, error) {
	query := `
	select
		id, user_id, total_amount, status, delivery_time
	from
		orders
	where
		deleted_at is null and kitchen_id = $1 and status = $2
	limit $3
	offset $4
	`

	rows, err := o.DB.QueryContext(ctx, query,
		f.KitchenId, f.Status, f.Pagination.Limit, f.Pagination.Offset,
	)
	if err != nil {
		return nil, errors.Wrap(err, "retrieval failure")
	}
	defer rows.Close()

	var orders []*pb.OrderKitchen
	for rows.Next() {
		var ord pb.OrderKitchen

		err := rows.Scan(&ord.Id, &ord.UserName, &ord.TotalAmount,
			&ord.Status, &ord.DeliveryTime,
		)
		if err != nil {
			return nil, errors.Wrap(err, "reading failure")
		}

		orders = append(orders, &ord)
	}

	return orders, nil
}

func (o *OrderRepo) GetPrice(ctx context.Context, id string) (float32, error) {
	query := "select price from dishes where deleted_at is null and id = $1"
	var price float32

	err := o.DB.QueryRowContext(ctx, query, id).Scan(&price)
	if err != nil {
		return -1, errors.Wrap(err, "price retrieval failure")
	}

	return price, nil
}

func (o *OrderRepo) GetName(ctx context.Context, id string) (string, error) {
	query := "select name from dishes where deleted_at is null and id = $1"
	var name string

	err := o.DB.QueryRowContext(ctx, query, id).Scan(&name)
	if err != nil {
		return "", errors.Wrap(err, "price retrieval failure")
	}

	return name, nil
}

func (o *OrderRepo) GetKitchenID(ctx context.Context, id string) (string, error) {
	query := `
	select
		kitchen_id
	from
		orders
	where
		deleted_at is null and id = $1
	`

	var kitchenID string
	err := o.DB.QueryRowContext(ctx, query, id).Scan(&kitchenID)
	if err != nil {
		return "", errors.Wrap(err, "reading failure")
	}

	return kitchenID, nil
}
