package fixer

import (
	"context"
	"errors"
	"log/slog"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/udugong/migrator"
	"github.com/udugong/migrator/events"
)

type OverrideFixer[T migrator.Entity[K], K migrator.IntegerOrString] struct {
	base             *gorm.DB
	target           *gorm.DB
	upsertColumns    []string // 插入冲突时需要更新的列默认全部
	uniqueColumnName string   // 唯一键的列名默认 Id
	logger           *slog.Logger
}

func NewOverrideFixer[T migrator.Entity[K], K migrator.IntegerOrString](
	base *gorm.DB, target *gorm.DB, logger *slog.Logger, opts ...Option[T, K]) (*OverrideFixer[T, K], error) {
	fixer := &OverrideFixer[T, K]{
		base:             base,
		target:           target,
		uniqueColumnName: "Id",
		logger:           logger,
	}
	for _, opt := range opts {
		opt.apply(fixer)
	}
	if len(fixer.upsertColumns) != 0 {
		return fixer, nil
	}
	columns, err := fixer.getColumns()
	fixer.upsertColumns = columns
	return fixer, err
}

func WithUniqueColumnName[T migrator.Entity[K], K migrator.IntegerOrString](name string) Option[T, K] {
	return optionFunc[T, K](func(o *OverrideFixer[T, K]) {
		o.uniqueColumnName = name
	})
}

// WithUpsertColumns 插入冲突时需要更新的列 例如: id, updated_time
func WithUpsertColumns[T migrator.Entity[K], K migrator.IntegerOrString](columns []string) Option[T, K] {
	return optionFunc[T, K](func(o *OverrideFixer[T, K]) {
		o.upsertColumns = columns
	})
}

func (f *OverrideFixer[T, K]) Fix(ctx context.Context, evt events.InconsistentEvent[K]) error {
	switch evt.Type {
	case events.InconsistentEventTypeNEQ, events.InconsistentEventTypeTargetMissing:
		var t T
		err := f.base.WithContext(ctx).Model(new(T)).Where(f.uniqueColumnName+" = ?", evt.UniqueKey).First(&t).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			res := f.target.WithContext(ctx).Model(&t).Unscoped().Where(f.uniqueColumnName+" = ?", evt.UniqueKey).Delete(&t)
			if res.RowsAffected != 1 {
				f.logger.LogAttrs(ctx, slog.LevelError, "删除目标表数据失败", slog.Any("event", evt))
			}
			return res.Error
		case err == nil:
			// upsert
			// 目标表没有该数据就 insert
			// 目标表有该数据则 update
			modifiedData := t.Modify().(T)
			return f.target.WithContext(ctx).Model(new(T)).Clauses(clause.OnConflict{
				DoUpdates: clause.AssignmentColumns(f.upsertColumns),
			}).Create(&modifiedData).Error
		default:
			return err
		}
	case events.InconsistentEventTypeBaseMissing:
		res := f.target.WithContext(ctx).Model(new(T)).Unscoped().Where(f.uniqueColumnName+" = ?", evt.UniqueKey).Delete(new(T))
		if res.RowsAffected != 1 {
			f.logger.LogAttrs(ctx, slog.LevelError, "删除目标表数据失败", slog.Any("event", evt))
		}
		return res.Error
	}
	return nil
}

func (f *OverrideFixer[T, K]) getColumns() ([]string, error) {
	rows, err := f.base.Model(new(T)).Rows()
	if err != nil {
		return nil, err
	}
	return rows.Columns()
}

type Option[T migrator.Entity[K], K migrator.IntegerOrString] interface {
	apply(*OverrideFixer[T, K])
}

type optionFunc[T migrator.Entity[K], K migrator.IntegerOrString] func(*OverrideFixer[T, K])

func (f optionFunc[T, K]) apply(o *OverrideFixer[T, K]) {
	f(o)
}
