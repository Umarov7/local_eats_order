package postgres

import (
	"log"
	"order-service/config"
)

func reviewDB() *ReviewRepo {
	db, err := ConnectDB(&config.Config{
		DB_HOST:     "localhost",
		DB_PORT:     "5432",
		DB_USER:     "postgres",
		DB_NAME:     "local_eats_order",
		DB_PASSWORD: "root",
	})
	if err != nil {
		log.Fatal("could not connect to postgres")
	}

	return NewReviewRepo(db)
}
