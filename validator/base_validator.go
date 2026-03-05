package validator

import (
	"context"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/udugong/migrator"
	"github.com/udugong/migrator/events"
)

type baseValidator[T migrator.IntegerOrString] struct {
	base   *gorm.DB
	target *gorm.DB
	// direction 表示修复数据以什么为准
	// SRC: 以源表为准
	// DST: 以目标表为准
	direction migrator.Direction
	producer  events.Producer[T]
	logger    *slog.Logger
}

// notify 上报不一致的数据
func (v *baseValidator[T]) notify(uniqueKey T, typ string) {
	event := events.InconsistentEvent[T]{
		Direction: v.direction,
		UniqueKey: uniqueKey,
		Type:      typ,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := v.producer.ProduceInconsistentEvent(ctx, event)
	if err != nil {
		v.logger.LogAttrs(ctx, slog.LevelError, "发送消息失败",
			slog.Any("event", event),
			slog.Any("err", err),
		)
	}
}

// batchNotify 批量上报不一致数据
func (v *baseValidator[T]) batchNotify(uniqueKeys []T, typ string) {
	evts := make([]events.InconsistentEvent[T], 0, len(uniqueKeys))
	for _, uniqueKey := range uniqueKeys {
		evts = append(evts, events.InconsistentEvent[T]{
			Direction: v.direction,
			UniqueKey: uniqueKey,
			Type:      typ,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := v.producer.ProduceInconsistentEvents(ctx, evts)
	if err != nil {
		v.logger.LogAttrs(ctx, slog.LevelError, "发送消息失败",
			slog.Any("events", evts),
			slog.Any("err", err),
		)
	}
}
