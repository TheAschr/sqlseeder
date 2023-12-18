package seeders

import (
	"encoding/json"
	"fmt"

	"github.com/TheAschr/sqlseeder"
)

func NewUsers(fileName string, children []sqlseeder.Config) sqlseeder.Config {
	return sqlseeder.Config{
		FileName:  fileName,
		ChunkSize: 100,
		HandleLine: func(batch *sqlseeder.Batch, line []byte) error {
			type User struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			}

			var user User

			if err := json.Unmarshal(line, &user); err != nil {
				return fmt.Errorf("failed to unmarshall user from line: %w", err)
			}

			batch.Queue(`
	INSERT INTO "User" (
		"id", 
		"name"
	) VALUES (
		$1,
		$2
	) ON CONFLICT ("id") DO UPDATE SET
		"name" = $2
`,
				user.ID,
				user.Name,
			)

			return nil
		},
		Children: children,
	}
}
