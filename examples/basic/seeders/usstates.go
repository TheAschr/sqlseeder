package seeders

import (
	"encoding/json"
	"fmt"

	"github.com/TheAschr/sqlseeder"
	"github.com/google/uuid"
)

var usStateIdNS = uuid.MustParse("cf8fa300-d478-4b1d-8c2c-60db1ef1c6c8")

func newUsStateID(stateAlpha string) uuid.UUID {
	return uuid.NewSHA1(usStateIdNS, []byte(stateAlpha))
}

func NewUsStates(fileName string, children []sqlseeder.Config) sqlseeder.Config {
	return sqlseeder.Config{
		FileName:  fileName,
		ChunkSize: 100,
		HandleLine: func(batch *sqlseeder.Batch, line []byte) error {
			type Properties struct {
				Alpha    string `json:"alpha"`
				Name     string `json:"name"`
				FipsCode string `json:"fips-code"`
			}

			type Feature struct {
				Properties Properties  `json:"properties"`
				Geometry   interface{} `json:"geometry"`
			}

			var feature Feature

			if err := json.Unmarshal(line, &feature); err != nil {
				return fmt.Errorf("failed to unmarshall feature from line: %w", err)
			}

			id := newUsStateID(feature.Properties.Alpha)

			geometry, err := json.Marshal(feature.Geometry)
			if err != nil {
				return fmt.Errorf("failed to marshal geometry: %w", err)
			}

			batch.Queue(`
	INSERT INTO "UsState" (
		"id", 
		"alpha",
		"name",
		"fipsCode",
		"shapeGeoJSON"
	) VALUES (
		$1,
		$2,
		$3,
		$4,
		$5::text::jsonb
	) ON CONFLICT ("id") DO UPDATE SET
		"alpha" = $2,
		"name" = $3,
		"fipsCode" = $4,
		"shapeGeoJSON" = $5::text::jsonb
`,
				id,
				feature.Properties.Alpha,
				feature.Properties.Name,
				feature.Properties.FipsCode,
				geometry,
			)

			return nil
		},
		Children: children,
	}
}
