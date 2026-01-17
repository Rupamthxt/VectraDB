package main

import (
	"fmt"
	"log"
	"os"

	"github.com/rupamthxt/vectradb/internal/store"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"

	vectorHttp "github.com/rupamthxt/vectradb/internal/http"
)

const SnapshotPath = "./vectradb.snap"

func main() {
	fmt.Println("Initializing VectraDB (High-Perf) mode...")

	var db *store.VectraDB
	var err error

	if _, err = os.Stat(SnapshotPath); err == nil {
		fmt.Println("Found snapshot. Loading data from disk...")
		db, err = store.LoadVectraDB(SnapshotPath)
		if err != nil {
			log.Fatalf("Failed to load snapshot: %v", err)
		}
		fmt.Println("Snapshot loaded successfully.")
	} else {
		fmt.Println("No snapshot found. Starting fresh.")
		db = store.NewVectraDB(3)
	}

	app := fiber.New()
	app.Use(logger.New())

	handler := vectorHttp.NewHandler(db)

	api := app.Group("/api/v1")
	api.Post("/insert", handler.Insert)
	api.Post("/search", handler.Search)

	admin := app.Group("/admin")
	admin.Post("/save", handler.SaveToDisk)
	admin.Post("/index", handler.CreateIndex)

	log.Println("VectraDB listening on port : 8080")
	log.Fatal(app.Listen(":8080"))
}
