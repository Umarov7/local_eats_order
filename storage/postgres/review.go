package postgres

import (
	"context"
	"database/sql"
	pb "order-service/genproto/review"

	"github.com/pkg/errors"
)

type ReviewRepo struct {
	DB *sql.DB
}

func NewReviewRepo(db *sql.DB) *ReviewRepo {
	return &ReviewRepo{DB: db}
}

func (r *ReviewRepo) Create(ctx context.Context, data *pb.NewReview) (*pb.NewReviewResp, error) {
	query := `
	insert into
		reviews (order_id, user_id, kitchen_id, rating, comment)
	values
		($1, $2, $3, $4, $5)
	returning
		id, order_id, user_id, kitchen_id, rating, comment, created_at
	`
	userID, kitchenID, err := r.GetIDs(ctx, data.OrderId)
	if err != nil {
		return nil, err
	}

	var rev pb.NewReviewResp

	err = r.DB.QueryRowContext(ctx, query, data.OrderId,
		userID, kitchenID, data.Rating, data.Comment).Scan(&rev.Id, &rev.OrderId, &rev.UserId,
		&rev.KitchenId, &rev.Rating, &rev.Comment, &rev.CreatedAt,
	)
	if err != nil {
		return nil, errors.Wrap(err, "insertion failure")
	}

	return &rev, nil
}

func (r *ReviewRepo) GetKitchenReviews(ctx context.Context, f *pb.Filter) (*pb.Reviews, error) {
	query := `
	select
		id, user_id, rating, comment, created_at
	from
		reviews
	where
		kitchen_id = $1
	limit $2
	offset $3
	`

	rows, err := r.DB.QueryContext(ctx, query, f.KitchenId, f.Limit, f.Offset)
	if err != nil {
		return nil, errors.Wrap(err, "retrieval failure")
	}
	defer rows.Close()

	var reviews []*pb.ReviewDetails
	for rows.Next() {
		var rev pb.ReviewDetails

		err := rows.Scan(&rev.Id, &rev.UserName, &rev.Rating, &rev.Comment, &rev.CreatedAt)
		if err != nil {
			return nil, errors.Wrap(err, "reading failure")
		}

		reviews = append(reviews, &rev)
	}

	kitchenRevs, err := r.CountRows(ctx, f.KitchenId)
	if err != nil {
		return nil, err
	}

	avgRating, err := r.GetAvgRating(ctx, f.KitchenId)
	if err != nil {
		return nil, err
	}

	return &pb.Reviews{
		Reviews:       reviews,
		Total:         int32(kitchenRevs),
		AverageRating: avgRating,
		Page:          f.Offset / f.Limit,
		Limit:         f.Limit,
	}, nil
}

func (r *ReviewRepo) GetIDs(ctx context.Context, id string) (string, string, error) {
	query := `
	select
		user_id, kitchen_id
	from
		orders
	where
		deleted_at is null and id = $1`

	var userID, kitchenID string
	err := r.DB.QueryRowContext(ctx, query, id).Scan(&userID, &kitchenID)
	if err != nil {
		return "", "", errors.Wrap(err, "ids retrieval failure")
	}

	return userID, kitchenID, nil
}

func (r *ReviewRepo) CountRows(ctx context.Context, kitchenID string) (int, error) {
	var rowsNum int
	query := "select count(1) from reviews where kitchen_id = $1"

	err := r.DB.QueryRowContext(ctx, query, kitchenID).Scan(&rowsNum)
	if err != nil {
		return -1, errors.Wrap(err, "rows counting failure")
	}

	return rowsNum, nil
}

func (r *ReviewRepo) GetAvgRating(ctx context.Context, kitchenID string) (float32, error) {
	var avg float32
	query := "select avg(rating) from reviews where kitchen_id = $1"

	err := r.DB.QueryRowContext(ctx, query, kitchenID).Scan(&avg)
	if err != nil {
		return -1, errors.Wrap(err, "average rating failure")
	}

	return avg, nil
}
