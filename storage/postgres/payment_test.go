package postgres

import (
	"log"
	"order-service/config"
)

func paymentDB() *PaymentRepo {
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

	return NewPaymentRepo(db)
}
