package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	pb "order-service/genproto/dish"
	"strconv"

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
		dishes (kitchen_id, name, description, price, category, ingredients, available)
	values
		($1, $2, $3, $4, $5, $6, $7)
	returning
		id, kitchen_id, name, description, price, category, ingredients, available, created_at
	`

	var ings pq.StringArray
	row := d.DB.QueryRowContext(ctx, query, data.KitchenId, data.Name, data.Description,
		data.Price, data.Category, pq.Array(data.Ingredients), data.Available,
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
		kitchen_id, name, description, price, category, ingredients,
		allergens, nutrition_info, dietary_info, available, created_at, updated_at 
	from
		dishes
	where
		deleted_at is null and id = $1
	`

	dish := pb.DishInfo{Id: id.Id}
	var ings, aller, diet pq.StringArray
	var nutrition []byte
	var nutInfo NutritionInfo

	err := d.DB.QueryRowContext(ctx, query, dish.Id).Scan(&dish.KitchenId, &dish.Name,
		&dish.Description, &dish.Price, &dish.Category, &ings, &aller,
		&nutrition, &diet, &dish.Available, &dish.CreatedAt, &dish.UpdatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(err, "reading failure")
	}

	dish.Ingredients = []string(ings)
	dish.Allergens = []string(aller)
	dish.DietaryInfo = []string(diet)
	err = json.Unmarshal(nutrition, &nutInfo)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling failure")
	}
	dish.NutritionInfo = []string{
		"calories: " + strconv.Itoa(nutInfo.Calories),
		"fat: " + strconv.Itoa(nutInfo.Fat),
		"carbs: " + strconv.Itoa(nutInfo.Carbs),
	}

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
