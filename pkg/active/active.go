package active

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	_ "github.com/lib/pq"
)

type (
	Params struct {
		Data struct{}
	}

	// Binary data
	Item struct {
		V types.JSONText
		E error
	}

	// Reference
	Ref struct {
		RowId      string
		ColumnName string
		CreatedAt  time.Time
		UpdatedAt  time.Time
		Version    uint
	}

	// Base Model
	Model interface {

		// Convert into bynary model
		Marshall() Item

		// Bind bynary and populate model
		Unmarshall(ref Ref, data types.JSONText) error
	}

	// Base class entity
	Entity struct {
		Model
		Ref Ref
	}

	// Batch of model changes
	Batch struct {
		add    []*Entity
		update []*Entity
	}

	// Single change
	Change struct {
		V *Entity
		T ChangeType
	}

	ChangeType int

	Action interface {
		Exec(params Params, batch Batch)
	}
)

const (
	AddChangeType = ChangeType(iota)
	UpdateChangeType
)

var (
	ErrOptimisticLock               = errors.New("model: optimistic lock")
	_defaultLvl       sql.TxOptions = sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false}
)

// All chages available in batch
func (b *Batch) Items() []Change {
	var arr []Change
	for _, e := range b.add {
		arr = append(arr, Change{V: e, T: AddChangeType})
	}
	for _, e := range b.update {
		arr = append(arr, Change{V: e, T: UpdateChangeType})
	}
	return arr
}

type pg struct {
	db *sqlx.DB
}

func (pg *pg) ApplyChanges(batch Batch) error {
	return pg.inTx(context.Background(), func(tx *sqlx.Tx) error {
		for _, change := range batch.Items() {
			switch change.T {
			case AddChangeType:
				if err := add(tx, change.V); err != nil {
					return err
				}
			case UpdateChangeType:
				if err := update(tx, change.V); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (p *pg) inTx(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	if tx, err := p.db.BeginTxx(ctx, &_defaultLvl); err != nil {
		return err
	} else {
		if err := fn(tx); err != nil {
			defer tx.Rollback()
			return err
		} else {
			return tx.Commit()
		}
	}
}

const (
	sqlActionsInsert = `INSERT INTO action_models (row_id, name, data, created_at) VALUES ($1, $2, $3, $4)`

	sqlGet    = `SELECT row_id, column_name, version, data, created_at, updated_at FROM models WHERE row_id = $1 AND column_name = $2`
	sqlInsert = `INSERT INTO models (row_id, column_name, version, data, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)`
	sqlUpdate = `UPDATE models 
		SET data = $1, version = $2, updated_at = $3 
		WHERE row_id = $4 AND column_name = $5 AND version = $6`
)

func get(tx *sqlx.DB, row, col string) error {
	aCell := &cell{}
	if err := tx.Get(aCell, sqlGet, row, col); err != nil {
		return err
	}
	return nil
}

type cell struct {
}

func add(tx *sqlx.Tx, entity *Entity) error {
	if item := entity.Marshall(); item.E != nil {
		return item.E
	} else if _, err := tx.Exec(sqlInsert,
		entity.Ref.RowId,
		entity.Ref.ColumnName,
		entity.Ref.Version,
		item.V,
		entity.Ref.CreatedAt,
		entity.Ref.UpdatedAt); err != nil {
		return err
	}
	return nil
}

func update(tx *sqlx.Tx, entity *Entity) error {
	if item := entity.Marshall(); item.E != nil {
		return item.E
	} else if r, err := tx.Exec(sqlUpdate,
		item.V,
		entity.Ref.Version+1,
		entity.Ref.UpdatedAt,
		entity.Ref.RowId,
		entity.Ref.ColumnName,
		entity.Ref.Version); err != nil {
		return err
	} else if num, err := r.RowsAffected(); err != nil {
		return err
	} else {
		switch num {
		case 1:
			return nil
		case 0:
			return ErrOptimisticLock
		default:
			return errors.New("panic: more then one record updated")
		}

	}
}

func writeLog(db *sqlx.DB, name string, params Params) error {
	b, err := json.Marshal(params.Data)
	if err != nil {
		return err
	}
	_, err = db.Exec(sqlActionsInsert, uuid.NewString(), name, b, time.Now())
	return err
}

// func runAction(pg *pg, action Action, params Params) {
// 	batch := Batch{}

// 	pg.inTx(context.Background(), func(tx *sqlx.Tx) error {

// 	})

// }
