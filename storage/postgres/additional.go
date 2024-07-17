package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	pbd "order-service/genproto/dish"
	pb "order-service/genproto/extra"
	pbo "order-service/genproto/order"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type BonusRepo struct {
	DB       *sql.DB
	dishRepo *DishRepo
}

func NewBonusRepo(db *sql.DB) *BonusRepo {
	return &BonusRepo{DB: db}
}

func (b *BonusRepo) GetKitchenStatistics(ctx context.Context, period *pb.Period) (*pb.Statistics, error) {
	query1 := `
	select
		count(1) as total_orders,
		sum(total_amount) as total_revenue
	from
		orders
	where
		deleted_at is null and status = 'completed' and
		kitchen_id = $1 and created_at between $2 and $3
	`

	query2 := `
	select
		avg(rating) as average_rating
	from
		reviews
	where
		kitchen_id = $1 and created_at between $2 and $3
	`

	query3 := `
	select
		items
	from
		orders
	where
		deleted_at is null and status = 'completed' and
		kitchen_id = $1 and created_at between $2 and $3
	`

	query4 := `
	select
		extract(hour from created_at), count(1) as orders_count
	from
		orders
	where
		deleted_at is null and status = 'completed' and
		kitchen_id = $1 and created_at between $2 and $3
	group by
		created_at
	order by
		orders_count desc
	limit
		3
	`

	var totalOrders int
	var totalRevenue, avgRating float32
	var itemsByte, hoursByte []byte
	var items []*pbo.Item
	dish := make(map[string]*pb.DishNoID)
	hour := make(map[int32]*pb.OrderPerHour)
	var topDishes []*pb.Dish
	var busiestHours []*pb.OrderPerHour

	err := b.DB.QueryRowContext(ctx, query1, period.Id, period.StartDate, period.EndDate).Scan(&totalOrders, &totalRevenue)
	if err != nil {
		return nil, errors.Wrap(err, "total orders and revenue retrieval failure")
	}
	err = b.DB.QueryRowContext(ctx, query2, period.Id, period.StartDate, period.EndDate).Scan(&avgRating)
	if err != nil {
		return nil, errors.Wrap(err, "average rating retrieval failure")
	}
	err = b.DB.QueryRowContext(ctx, query3, period.Id, period.StartDate, period.EndDate).Scan(&itemsByte)
	if err != nil {
		return nil, errors.Wrap(err, "top dishes retrieval failure")
	}
	err = json.Unmarshal(itemsByte, &items)
	if err != nil {
		return nil, errors.Wrap(err, "top dishes unmarshal failure")
	}
	err = b.DB.QueryRowContext(ctx, query4, period.Id, period.StartDate, period.EndDate).Scan(&hoursByte)
	if err != nil {
		return nil, errors.Wrap(err, "busiest hours retrieval failure")
	}
	err = json.Unmarshal(hoursByte, &busiestHours)
	if err != nil {
		return nil, errors.Wrap(err, "busiest hours unmarshal failure")
	}

	for _, v := range items {
		name, price, err := b.dishRepo.GetNamePrice(ctx, v.DishId)
		if err != nil {
			return nil, errors.Wrap(err, "dish's name and price retrieval failure")
		}

		if existingDish, ok := dish[v.DishId]; ok {
			existingDish.OrdersCount += v.Quantity
			existingDish.Revenue += float32(v.Quantity) * price
			dish[v.DishId] = existingDish
		} else {
			newDish := pb.DishNoID{
				Name:        name,
				OrdersCount: v.Quantity,
				Revenue:     float32(v.Quantity) * price,
			}
			dish[v.DishId] = &newDish
		}
	}

	for _, v := range busiestHours {
		if existingHour, ok := hour[v.Hour]; ok {
			existingHour.OrdersCount += v.OrdersCount
			hour[v.Hour] = existingHour
		} else {
			hour[v.Hour] = v
		}
	}

	for k, v := range dish {
		topDishes = append(topDishes, &pb.Dish{
			Id:          k,
			Name:        v.Name,
			OrdersCount: v.OrdersCount,
			Revenue:     v.Revenue,
		})
	}

	for k, v := range hour {
		busiestHours = append(busiestHours, &pb.OrderPerHour{
			Hour:        k,
			OrdersCount: v.OrdersCount,
		})
	}

	return &pb.Statistics{
		TotalOrders:   int32(totalOrders),
		TotalRevenue:  totalRevenue,
		AverageRating: avgRating,
		TopDishes:     topDishes,
		BusiestHours:  busiestHours,
	}, nil
}

func (b *BonusRepo) TrackActivity(ctx context.Context, period *pb.Period) (*pb.Activity, error) {
	query1 := `
	select
		count(1) as total_orders,
		sum(total_amount) as total_spent
	from
		orders
	where
		deleted_at is null and status = 'completed' and
		user_id = $1 and created_at between $2 and $3
	`

	query2 := `
	select
		items
	from
		orders
	where
		deleted_at is null and status = 'completed' and
		user_id = $1 and created_at between $2 and $3
	`

	query3 := `
	select
		kitchen_id, kitchen_id
		count(1) as orders_count
	from
		orders
	where
		deleted_at is null and status = 'completed' and
		user_id = $1 and created_at between $2 and $3
	`

	var totalOrders int
	var totalSpent float32
	var itemsByte, kitchensByte []byte
	var items []*pbo.Item
	cuisine := make(map[string]int32)
	kitchen := make(map[string]*pb.KitchenNoID)
	var kitchens []*pb.Kitchen
	var cuisines []*pb.Cuisine

	err := b.DB.QueryRowContext(ctx, query1, period.Id, period.StartDate, period.EndDate).Scan(&totalOrders, &totalSpent)
	if err != nil {
		return nil, errors.Wrap(err, "total orders and revenue retrieval failure")
	}
	err = b.DB.QueryRowContext(ctx, query2, period.Id, period.StartDate, period.EndDate).Scan(&itemsByte)
	if err != nil {
		return nil, errors.Wrap(err, "dishes retrieval failure")
	}
	err = json.Unmarshal(itemsByte, &items)
	if err != nil {
		return nil, errors.Wrap(err, "dishes unmarshal failure")
	}
	err = b.DB.QueryRowContext(ctx, query3, period.Id, period.StartDate, period.EndDate).Scan(&kitchensByte)
	if err != nil {
		return nil, errors.Wrap(err, "kitchens retrieval failure")
	}
	err = json.Unmarshal(kitchensByte, &kitchens)
	if err != nil {
		return nil, errors.Wrap(err, "kitchens unmarshal failure")
	}

	for _, v := range items {
		categ, err := b.dishRepo.GetCategory(ctx, v.DishId)
		if err != nil {
			return nil, errors.Wrap(err, "category retrieval failure")
		}

		if existingCuisine, ok := cuisine[categ]; ok {
			existingCuisine += v.Quantity
			cuisine[categ] = existingCuisine
		} else {
			cuisine[categ] = v.Quantity
		}
	}

	for _, v := range kitchens {
		if existingKitchen, ok := kitchen[v.Id]; ok {
			existingKitchen.OrdersCount += v.OrdersCount
			kitchen[v.Id] = existingKitchen
		} else {
			newKitchen := pb.KitchenNoID{
				Name:        v.Name,
				OrdersCount: v.OrdersCount,
			}
			kitchen[v.Id] = &newKitchen
		}
	}

	for k, v := range cuisine {
		cuisines = append(cuisines, &pb.Cuisine{
			CuisineType: k,
			OrdersCount: v,
		})
	}

	for k, v := range kitchen {
		kitchens = append(kitchens, &pb.Kitchen{
			Id:          k,
			Name:        v.Name,
			OrdersCount: v.OrdersCount,
		})
	}

	return &pb.Activity{
		TotalOrders:      int32(totalOrders),
		TotalSpent:       totalSpent,
		FavoriteCuisines: cuisines,
		FavoriteKitchens: kitchens,
	}, nil
}

func (b *BonusRepo) SetWorkingHours(ctx context.Context, wh *pb.WorkingHours) (*pb.WorkingHoursResp, error) {
	query := `
	insert into
		working_hours (kitchen_id, day_of_week, open_time, close_time)
	values
		($1, $2, $3, $4)
	`

	days := map[string]int{
		"monday":    1,
		"tuesday":   2,
		"wednesday": 3,
		"thursday":  4,
		"friday":    5,
		"saturday":  6,
		"sunday":    7,
	}

	for k, v := range wh.Schedule {
		_, err := b.DB.ExecContext(ctx, query, wh.KitchenId, days[k], v.Open, v.Close)
		if err != nil {
			return nil, errors.Wrap(err, "working hours set failure")
		}
	}

	return &pb.WorkingHoursResp{
		KitchenId: wh.KitchenId,
		Schedule:  wh.Schedule,
		UpdatedAt: time.Now().String(),
	}, nil
}

func (b *BonusRepo) GetNutrition(ctx context.Context, id *pb.ID) (*pb.NutritionalInfo, error) {
	query := `
	select
		allergens, nutrition_info, dietary_info
	from
		dishes
	where
		deleted_at is null and id = $1
	`

	var aller, diet pq.StringArray
	var info []byte
	var nutInfo pbd.NutritionalInfo

	err := b.DB.QueryRowContext(ctx, query, id.Id).Scan(&aller, &info, &diet)
	if err != nil {
		return nil, errors.Wrap(err, "dishes retrieval failure")
	}
	err = json.Unmarshal(info, &nutInfo)
	if err != nil {
		return nil, errors.Wrap(err, "dishes unmarshal failure")
	}

	return &pb.NutritionalInfo{
		Allergens:   []string(aller),
		Calories:    nutInfo.Calories,
		Protein:     nutInfo.Protein,
		Fat:         nutInfo.Fat,
		Carbs:       nutInfo.Carbs,
		DietaryInfo: []string(diet),
	}, nil
}
