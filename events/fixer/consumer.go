package fixer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/udugong/kafka-gox/handler"

	"github.com/udugong/migrator"
	"github.com/udugong/migrator/events"
	"github.com/udugong/migrator/fixer"
)

type Consumer[T migrator.Entity[K], K migrator.IntegerOrString] struct {
	reader   *kafka.Reader
	srcFirst *fixer.OverrideFixer[T, K]
	dstFirst *fixer.OverrideFixer[T, K]
	logger   *slog.Logger
}

func NewConsumer[T migrator.Entity[K], K migrator.IntegerOrString](reader *kafka.Reader,
	srcFirst, dstFirst *fixer.OverrideFixer[T, K], logger *slog.Logger) (*Consumer[T, K], error) {
	return &Consumer[T, K]{
		reader:   reader,
		logger:   logger,
		srcFirst: srcFirst,
		dstFirst: dstFirst,
	}, nil
}

// Start 这边就是自己启动 goroutine 了
func (c *Consumer[T, K]) Start() error {
	go func() {
		ctx := context.Background()
		err := handler.NewHandler[events.InconsistentEvent[K]](c.logger, c.Consume).ReadMsg(ctx, c.reader)
		if err != nil {
			c.logger.LogAttrs(ctx, slog.LevelError, "退出了消费循环异常", slog.Any("err", err))
		}
	}()
	return nil
}

func (c *Consumer[T, K]) Consume(ctx context.Context, _ kafka.Message, evt events.InconsistentEvent[K]) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	switch evt.Direction {
	case migrator.SRC:
		return c.srcFirst.Fix(ctx, evt)
	case migrator.DST:
		return c.dstFirst.Fix(ctx, evt)
	}
	return errors.New("未知的校验方向")
}
