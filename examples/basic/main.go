package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/TheAschr/sqlseeder"
	"github.com/TheAschr/sqlseeder/examples/basic/seeders"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vbauerster/mpb/v8"
)

const dataDir = "./data"

func main() {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	if err != nil {
		log.Fatalf("Failed to create db connection: %v", err)
	}
	defer db.Close()

	if err := initDb(db); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	p := mpb.NewWithContext(ctx)

	s := sqlseeder.New(db, sqlseeder.WithProgress(p))

	startTime := time.Now()

	if err := s.Run(ctx, []sqlseeder.Config{
		seeders.NewUsers(
			path.Join(dataDir, "users.gz"),
			nil,
		),
		seeders.NewUsStates(
			path.Join(dataDir, "us-states.gz"),
			[]sqlseeder.Config{
				seeders.NewUsCounties(path.Join(dataDir, "us-counties.gz"), nil),
			},
		),
		seeders.NewEsriLandformPolygons(path.Join(dataDir, "esri-landform-polygons.gz"), nil),
	}); err != nil {
		log.Fatalf("Failed to seed database: %v", err)
	}

	fmt.Printf("Completed in %v\n", time.Since(startTime).Round(time.Millisecond))
}

func initDb(db *sql.DB) error {
	if _, err := db.ExecContext(context.Background(), `
	CREATE TABLE IF NOT EXISTS "User" (
		"id" INT NOT NULL,
		"name" TEXT NOT NULL,
	
		CONSTRAINT "User_pkey" PRIMARY KEY ("id")
	)`); err != nil {
		return fmt.Errorf("failed to create user table: %w", err)
	}

	if _, err := db.ExecContext(context.Background(), `
	CREATE TABLE IF NOT EXISTS "EsriLandformPolygon" (
		"id" TEXT NOT NULL,
		"featureCodeId" INTEGER NOT NULL,
		"gazId" INTEGER NOT NULL,
		"name" TEXT NOT NULL,
		"geoJSON" JSONB NOT NULL,
	
		CONSTRAINT "EsriLandformPolygon_pkey" PRIMARY KEY ("id")
	)`); err != nil {
		return fmt.Errorf("failed to create esri landform polygon table: %w", err)
	}

	if _, err := db.ExecContext(context.Background(), `
	CREATE TABLE IF NOT EXISTS "UsState" (
		"id" TEXT NOT NULL,
		"fipsCode" TEXT NOT NULL,
		"alpha" TEXT NOT NULL,
		"name" TEXT NOT NULL,
		"shapeGeoJSON" JSONB NOT NULL,
		"districtOfColumbiaId" TEXT,
	
		CONSTRAINT "UsState_pkey" PRIMARY KEY ("id")
	)`); err != nil {
		return fmt.Errorf("failed to create us state table: %w", err)
	}

	if _, err := db.ExecContext(context.Background(), `
	CREATE TABLE IF NOT EXISTS "UsCounty" (
		"id" TEXT NOT NULL,
		"stcoFipsCode" TEXT NOT NULL,
		"shortName" TEXT NOT NULL,
		"longName" TEXT NOT NULL,
		"deprecated" BOOLEAN NOT NULL DEFAULT false,
		"shapeGeoJSON" JSONB NOT NULL,
		"stateId" TEXT,
		"districtOfColumbiaId" TEXT,
		"territoryId" TEXT,
	
	    FOREIGN KEY ("stateId") REFERENCES "UsState"("id"),
		CONSTRAINT "UsCounty_pkey" PRIMARY KEY ("id")
	)`); err != nil {
		return fmt.Errorf("failed to create us county table: %w", err)
	}

	return nil
}
