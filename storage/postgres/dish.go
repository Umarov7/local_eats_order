package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	pb "order-service/genproto/dish"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type DishRepo struct {
	DB *sql.DB
}

func NewDishRepo(db *sql.DB) *DishRepo {
	return &DishRepo{DB: db}
}

func (d *DishRepo) Create(ctx context.Context, data *pb.NewDish) (*pb.NewDishResp, error) {
	query := `
	insert into
		dishes (kitchen_id, name, description, price, category, ingredients,
			allergens, nutrition_info, dietary_info, available)
	values
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	returning
		id, kitchen_id, name, description, price, category, ingredients, available, created_at
	`

	var ings pq.StringArray
	aller := pq.StringArray{"no_allergens"}
	nutritJSON := `{"calories": 0, "protein": 0, "fat": 0, "carbs": 0}`
	nutrit := json.RawMessage(nutritJSON)
	diet := pq.StringArray{"no_dietary_info"}
	row := d.DB.QueryRowContext(ctx, query, data.KitchenId, data.Name, data.Description,
		data.Price, data.Category, pq.Array(data.Ingredients),
		aller, nutrit, diet, data.Available,
	)

	var dish pb.NewDishResp
	err := row.Scan(&dish.Id, &dish.KitchenId, &dish.Name, &dish.Description, &dish.Price,
		&dish.Category, &ings, &dish.Available, &dish.CreatedAt)
	if err != nil {
		return nil, errors.Wrap(err, "insertion failure")
	}

	dish.Ingredients = []string(ings)
	return &dish, nil

}

func (d *DishRepo) Read(ctx context.Context, id *pb.ID) (*pb.DishInfo, error) {
	query := `
	select
		kitchen_id, name, description, price, category,
		ingredients, available, created_at, updated_at 
	from
		dishes
	where
		deleted_at is null and id = $1
	`

	dish := pb.DishInfo{Id: id.Id}
	var ings pq.StringArray

	err := d.DB.QueryRowContext(ctx, query, dish.Id).Scan(&dish.KitchenId, &dish.Name,
		&dish.Description, &dish.Price, &dish.Category, &ings, &dish.Available,
		&dish.CreatedAt, &dish.UpdatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(err, "reading failure")
	}

	dish.Ingredients = []string(ings)

	return &dish, nil
}

func (d *DishRepo) Update(ctx context.Context, data *pb.NewData) (*pb.UpdatedData, error) {
	query := `
	update
		dishes
	set
		name = $1, price = $2, available = $3, updated_at = NOW()
	where
		deleted_at is null and id = $4
	returning
		id, kitchen_id, name, description, price, category, ingredients, available, updated_at
	`
	var upData pb.UpdatedData
	var ings pq.StringArray

	row := d.DB.QueryRowContext(ctx, query, data.Name, data.Price, data.Available, data.Id)

	err := row.Scan(&upData.Id, &upData.KitchenId, &upData.Name,
		&upData.Description, &upData.Price, &upData.Category,
		&ings, &upData.Available, &upData.UpdatedAt)
	if err != nil {
		return nil, errors.Wrap(err, "update failure")
	}

	upData.Ingredients = []string(ings)
	return &upData, nil
}

func (d *DishRepo) Delete(ctx context.Context, id *pb.ID) error {
	query := `
	update
		dishes
	set
		deleted_at = NOW()
	where
		deleted_at is null and id = $1
	`

	res, err := d.DB.ExecContext(ctx, query, id.Id)
	if err != nil {
		return errors.Wrap(err, "deletion failure")
	}

	rowsNum, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "rows affected failure")
	}
	if rowsNum < 1 {
		return errors.Wrap(err, "no rows affected")
	}

	return nil
}

func (d *DishRepo) Fetch(ctx context.Context, pag *pb.Pagination) ([]*pb.DishDetails, error) {
	query := `
	select
		id, name, price, category, available
	from
		dishes
	where
		deleted_at is null
	limit $1
	offset $2
	`

	rows, err := d.DB.QueryContext(ctx, query, pag.Limit, pag.Offset)
	if err != nil {
		return nil, errors.Wrap(err, "retrieval failure")
	}
	defer rows.Close()

	var dishes []*pb.DishDetails
	for rows.Next() {
		var dsh pb.DishDetails

		err := rows.Scan(&dsh.Id, &dsh.Name, &dsh.Price, &dsh.Category, &dsh.Available)
		if err != nil {
			return nil, errors.Wrap(err, "reading failure")
		}

		dishes = append(dishes, &dsh)
	}

	return dishes, nil
}

func (d *DishRepo) CountRows(ctx context.Context) (int, error) {
	var rowsNum int
	query := "select count(1) from dishes where deleted_at is null"

	err := d.DB.QueryRowContext(ctx, query).Scan(&rowsNum)
	if err != nil {
		return -1, errors.Wrap(err, "rows counting failure")
	}

	return rowsNum, nil
}

func (d *DishRepo) GetNamePrice(ctx context.Context, id string) (string, float32, error) {
	query := "select name, price from dishes where deleted_at is null and id = $1"
	var name string
	var price float32

	err := d.DB.QueryRowContext(ctx, query, id).Scan(&name, &price)
	if err != nil {
		return "", -1, errors.Wrap(err, "name and price retrieval failure")
	}

	return name, price, nil
}

func (d *DishRepo) GetCategory(ctx context.Context, id string) (string, error) {
	query := `
	select
		category
	from
		dishes
	where
		deleted_at is null and id = $1`

	var category string
	err := d.DB.QueryRowContext(ctx, query, id).Scan(&category)
	if err != nil {
		return "", errors.Wrap(err, "category retrieval failure")
	}

	return category, nil
}
