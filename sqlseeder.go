package sqlseeder

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/TheAschr/sqlseeder/internal/filereader"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"
)

type SeederOption = func(s *Seeder)

// Enables progress bars for the seeder.
func WithProgress(p *mpb.Progress) SeederOption {
	return func(s *Seeder) {
		s.progress = p
	}
}

type Seeder struct {
	db       *sql.DB
	progress *mpb.Progress
}

/*
New creates a new sqlseeder. Available options are:

  - WithProgress : pass in an *mpb.Progress to get progress bars
*/
func New(db *sql.DB, opts ...SeederOption) *Seeder {
	s := Seeder{
		db: db,
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s
}

// Config represents a file to be seeded.
type Config struct {
	// The path to the file to be seeded
	FileName string
	// ChunkSize is the number of lines to be read at a time (defaults to 100)
	ChunkSize int
	// HandleLine is called for each line in the file.
	// If HandleLine returns an error, the seeder will stop running.
	HandleLine func(batch *Batch, line []byte) error
	// A slice of Configs that will be run after the parent Config has finished.
	Children []Config
}

// Run runs the seeder. It takes a context and a slice of Configs. Each Config
// represents a file to be seeded. The Configs are run concurrently, and each
// Config can have children Configs. The children Configs are run after the
// parent Config has finished.
func (s *Seeder) Run(ctx context.Context, cfgs []Config) error {
	eg, ctx := errgroup.WithContext(ctx)

	for _, cfg := range cfgs {
		cfg := cfg

		eg.Go(func() error {
			fr, err := filereader.New(cfg.FileName)
			if err != nil {
				return fmt.Errorf("failed to create new file reader for '%s': %w", cfg.FileName, err)
			}

			label := strings.SplitN(path.Base(cfg.FileName), ".", 2)[0]

			var bar *mpb.Bar
			if s.progress != nil {

				numLines, err := fr.TotalLines()
				if err != nil {
					return fmt.Errorf("failed to get number of lines in file '%s': %w", cfg.FileName, err)
				}

				bar = s.progress.AddBar(numLines,
					mpb.PrependDecorators(
						decor.Name(label, decor.WCSyncSpaceR),
						decor.Percentage(decor.WCSyncSpace),
					),
					mpb.AppendDecorators(
						decor.OnComplete(
							decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncWidth), "done",
						),
					),
				)
			}

			var chunkSize int
			if cfg.ChunkSize != 0 {
				chunkSize = cfg.ChunkSize
			} else {
				chunkSize = 100
			}

			startTime := time.Now()

			for {
				chunkStartTime := time.Now()
				chunk, err := fr.ReadLines(chunkSize)
				if err != nil {
					return fmt.Errorf("failed to read lines for '%s': %w", cfg.FileName, err)
				}
				if len(chunk) == 0 {
					break
				}

				batch := &Batch{}

				for _, line := range chunk {
					if err := cfg.HandleLine(batch, line); err != nil {
						return err
					}
				}

				for _, q := range batch.queuedQueries {
					if _, err := s.db.ExecContext(ctx, q.query, q.arguments...); err != nil {
						return fmt.Errorf("failed to execute batch for '%s': %w", cfg.FileName, err)

					}
				}

				if bar != nil {
					bar.EwmaIncrBy(len(chunk), time.Since(chunkStartTime))
				}
			}

			if bar != nil {
				bar.Wait()
			} else {
				fmt.Printf("Finished seeding %s in %v\n", label, time.Since(startTime).Round(time.Millisecond))
			}

			return s.Run(ctx, cfg.Children)
		})

	}

	return eg.Wait()
}

type QueuedQuery struct {
	query     string
	arguments []any
}

type Batch struct {
	queuedQueries []*QueuedQuery
}

func (b *Batch) Len() int {
	return len(b.queuedQueries)
}

func (b *Batch) Queue(query string, arguments ...any) *QueuedQuery {
	q := QueuedQuery{query, arguments}
	b.queuedQueries = append(b.queuedQueries, &q)
	return &q
}
