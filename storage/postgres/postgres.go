package postgres

import (
	"database/sql"
	"fmt"
	"order-service/config"

	_ "github.com/lib/pq"
)

func ConnectDB(cfg *config.Config) (*sql.DB, error) {
	conn := fmt.Sprintf("port = %s host=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DB_PORT, cfg.DB_HOST, cfg.DB_USER, cfg.DB_PASSWORD, cfg.DB_NAME)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

type NutritionInfo struct {
	Calories int `json:"calories"`
	Fat      int `json:"fat"`
	Carbs    int `json:"carbs"`
}

type Item struct {
	DishId   string `json:"dish_id"`
	Quantity int    `json:"quantity"`
}
