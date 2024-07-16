package postgres

import (
	"context"
	"database/sql"
	pb "order-service/genproto/payment"

	"github.com/pkg/errors"
)

type PaymentRepo struct {
	DB *sql.DB
}

func NewPaymentRepo(db *sql.DB) *PaymentRepo {
	return &PaymentRepo{DB: db}
}

func (p *PaymentRepo) Create(ctx context.Context, data *pb.NewPayment) (*pb.NewPaymentResp, error) {
	query := `
	insert into
		payments (order_id, amount, method, card_number, expiry_date, cvv)
	values
		($1, $2, $3, $4, $5, $6)
	returning
		id, order_id, amount, status, transaction_id, created_at
	`

	amount, err := p.GetAmount(ctx, data.OrderId)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get amount")
	}

	row := p.DB.QueryRowContext(ctx, query, data.OrderId, amount, data.PaymentMethod,
		data.CardNumber, data.ExpiryDate, data.Cvv,
	)

	var pay pb.NewPaymentResp
	var trID sql.NullString
	err = row.Scan(&pay.Id, &pay.OrderId, &pay.Amount, &pay.Status, &trID, &pay.CreatedAt)
	if err != nil {
		return nil, errors.Wrap(err, "insertion failure")
	}

	if trID.Valid {
		pay.TransactionId = trID.String
	}

	return &pay, nil
}

func (p *PaymentRepo) Read(ctx context.Context, id *pb.ID) (*pb.PaymentDetails, error) {
	query := `
	select
		order_id, amount, status, method, card_number, expiry_date, cvv, transaction_id, created_at
	from
		payments
	where
		id = $1
	`

	var pay pb.PaymentDetails
	var cardNum, expDate, cvv, trID sql.NullString
	err := p.DB.QueryRowContext(ctx, query, id.Id).Scan(&pay.OrderId, &pay.Amount, &pay.Status,
		&pay.Method, &cardNum, &expDate, &cvv, &trID, &pay.CreatedAt)
	if err != nil {
		return nil, errors.Wrap(err, "reading failure")
	}

	pay.Id = id.Id
	if cardNum.Valid {
		pay.CardNumber = cardNum.String
	}
	if expDate.Valid {
		pay.ExpiryDate = expDate.String
	}
	if cvv.Valid {
		pay.Cvv = cvv.String
	}
	if trID.Valid {
		pay.TransactionId = trID.String
	}
	return &pay, nil
}

func (p *PaymentRepo) GetAmount(ctx context.Context, id string) (float32, error) {
	query := `
	select
		total_amount
	from
		orders
	where
		deleted_at is null and id = $1
	`

	var amount float32
	err := p.DB.QueryRowContext(ctx, query, id).Scan(&amount)
	if err != nil {
		return 0, errors.Wrap(err, "reading failure")
	}

	return amount, nil
}
