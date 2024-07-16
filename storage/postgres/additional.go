package postgres

import "database/sql"

type BonusRepo struct {
	DB *sql.DB
}

func NewBonusRepo(db *sql.DB) *BonusRepo {
	return &BonusRepo{DB: db}
}

