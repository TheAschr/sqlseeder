package seeders

import (
	"encoding/json"
	"fmt"

	"github.com/TheAschr/sqlseeder"
	"github.com/google/uuid"
)

var territoryAbbreviations = []string{
	"GU", "PR", "VI", "AS", "MP",
}

var usCountyIdNS = uuid.MustParse("8b3fa54b-f47d-4e85-a177-962ce30129cf")

func newUsCountyID(longName string, stateAbbr string) uuid.UUID {
	return uuid.NewSHA1(usCountyIdNS, []byte(fmt.Sprintf("%s,%s", longName, stateAbbr)))
}

func NewUsCounties(fileName string, children []sqlseeder.Config) sqlseeder.Config {
	return sqlseeder.Config{
		FileName:  fileName,
		ChunkSize: 100,
		HandleLine: func(batch *sqlseeder.Batch, line []byte) error {
			type Properties struct {
				StateUsAbbreviation string `json:"stusab"`
				GeoID               string `json:"geoid"`
				NameLSAD            string `json:"namelsad"`
				Name                string `json:"name"`
			}

			type Feature struct {
				Properties Properties  `json:"properties"`
				Geometry   interface{} `json:"geometry"`
			}

			var feature Feature

			if err := json.Unmarshal(line, &feature); err != nil {
				return fmt.Errorf("failed to unmarshall feature from line: %w", err)
			}

			id := newUsCountyID(feature.Properties.NameLSAD, feature.Properties.StateUsAbbreviation)

			var isTerritory bool
			for _, t := range territoryAbbreviations {
				if t == feature.Properties.StateUsAbbreviation {
					isTerritory = true
					break
				}
			}

			var stateID *uuid.UUID
			if !isTerritory && feature.Properties.StateUsAbbreviation != "DC" {
				s := newUsStateID(feature.Properties.StateUsAbbreviation)
				stateID = &s
			}

			deprecated := false

			geometry, err := json.Marshal(feature.Geometry)
			if err != nil {
				return fmt.Errorf("failed to marshal geometry: %w", err)
			}

			batch.Queue(`
	INSERT INTO "UsCounty" (
		"id", 
		"stateId",
		"territoryId",
		"stcoFipsCode",
		"longName",
		"shortName",
		"shapeGeoJSON",
		"deprecated"
	) VALUES (
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7::text::jsonb,
		$8
	) ON CONFLICT ("id") DO UPDATE SET
		"stateId" = $2,
		"territoryId" = $3,
		"stcoFipsCode" = $4,
		"longName" = $5,
		"shortName" = $6,
		"shapeGeoJSON" = $7::text::jsonb,
		"deprecated" = $8
`,
				id,
				stateID,
				nil,
				feature.Properties.GeoID,
				feature.Properties.NameLSAD,
				feature.Properties.Name,
				geometry,
				deprecated,
			)

			return nil
		},
		Children: children,
	}
}
