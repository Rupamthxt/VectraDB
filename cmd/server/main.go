package main

import (
	"fmt"
	"log"

	//"os"

	"github.com/rupamthxt/vectradb/internal/store"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"

	vectorHttp "github.com/rupamthxt/vectradb/internal/http"
)

const SnapshotPath = "./vectradb.snap"

func main() {
	fmt.Println("Initializing VectraDB (High-Perf) mode...")

	baseDir := "./data"
	cluster, err := store.NewCluster(32, 128, baseDir)
	if err != nil {
		log.Fatal(err)
	}

	app := fiber.New()
	app.Use(logger.New())

	handler := vectorHttp.NewHandler(cluster)

	api := app.Group("/api/v1")
	api.Post("/insert", handler.Insert)
	api.Post("/search", handler.Search)

	admin := app.Group("/admin")
	// admin.Post("/save", handler.SaveToDisk)
	admin.Post("/index", handler.CreateIndex)

	log.Println("VectraDB listening on port : 8080")
	log.Fatal(app.Listen(":8080"))
}
