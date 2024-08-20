package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cast"
)

type Config struct {
	AUTH_PORT   string
	ORDER_PORT  string
	DB_HOST     string
	DB_PORT     string
	DB_USER     string
	DB_NAME     string
	DB_PASSWORD string
	REDIS_ADDR  string
}

func Load() *Config {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("error while loading .env file: %v", err)
	}

	return &Config{
		AUTH_PORT:   cast.ToString(coalesce("AUTH_PORT", "auth-service:50051")),
		ORDER_PORT:  cast.ToString(coalesce("ORDER_PORT", "order-service:50052")),
		DB_HOST:     cast.ToString(coalesce("DB_HOST", "postgres2")),
		DB_PORT:     cast.ToString(coalesce("DB_PORT", "5432")),
		DB_USER:     cast.ToString(coalesce("DB_USER", "postgres")),
		DB_NAME:     cast.ToString(coalesce("DB_NAME", "order_service")),
		DB_PASSWORD: cast.ToString(coalesce("DB_PASSWORD", "password")),
		REDIS_ADDR:  cast.ToString(coalesce("REDIS_ADDR", "redis:6379")),
	}
}

func coalesce(key string, value interface{}) interface{} {
	val, exist := os.LookupEnv(key)
	if exist {
		return val
	}
	return value
}
