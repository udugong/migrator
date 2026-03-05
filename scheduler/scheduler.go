package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/udugong/gormx/connpool"

	"github.com/udugong/migrator"
	"github.com/udugong/migrator/validator"
)

type Scheduler[T migrator.Entity[K], K migrator.IntegerOrString] struct {
	lock         sync.Mutex
	srcValidator *validator.Validator[T, K]
	dstValidator *validator.Validator[T, K]
	pool         *connpool.DoubleWritePool
	pattern      string
	cancelFull   func()
	cancelIncr   func()
	logger       *slog.Logger
}

func NewScheduler[T migrator.Entity[K], K migrator.IntegerOrString](srcValidator, dstValidator *validator.Validator[T, K],
	pool *connpool.DoubleWritePool, logger *slog.Logger) *Scheduler[T, K] {
	return &Scheduler[T, K]{
		srcValidator: srcValidator,
		dstValidator: dstValidator,
		pattern:      connpool.PatternSrcOnly,
		cancelFull:   func() {},
		cancelIncr:   func() {},
		pool:         pool,
		logger:       logger,
	}
}

// ---- 迁移四个阶段 ---- //

// SrcOnly 只读写源表
func (s *Scheduler[T, K]) SrcOnly() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternSrcOnly
	return s.pool.UpdatePattern(connpool.PatternSrcOnly)
}

// SrcFirst 读写源表,同步写目标表
func (s *Scheduler[T, K]) SrcFirst() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternSrcFirst
	return s.pool.UpdatePattern(connpool.PatternSrcFirst)
}

// DstFirst 读写目标表,同步写源表
func (s *Scheduler[T, K]) DstFirst() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternDstFirst
	return s.pool.UpdatePattern(connpool.PatternDstFirst)
}

// DstOnly 只读写目标表
func (s *Scheduler[T, K]) DstOnly() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternDstOnly
	return s.pool.UpdatePattern(connpool.PatternDstOnly)
}

// StopIncrementValidation 停止增量校验
func (s *Scheduler[T, K]) StopIncrementValidation() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cancelIncr()
}

// StartIncrementValidation 开启增量校验
func (s *Scheduler[T, K]) StartIncrementValidation(updatedTime, interval int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	cancel := s.cancelIncr
	v, err := s.newValidator()
	if err != nil {
		return err
	}
	v.UpdatedTime(updatedTime).SleepInterval(time.Duration(interval) * time.Millisecond)

	go func() {
		var ctx context.Context
		ctx, s.cancelIncr = context.WithCancel(context.Background())
		cancel() // 取消上一次的增量校验
		err := v.Validate(ctx)
		s.logger.LogAttrs(ctx, slog.LevelWarn, "退出增量校验", slog.Any("err", err))
	}()
	return nil
}

// StopFullValidation 停止全量校验
func (s *Scheduler[T, K]) StopFullValidation() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cancelFull()
}

// StartFullValidation 开启全量校验
func (s *Scheduler[T, K]) StartFullValidation() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	cancel := s.cancelFull
	v, err := s.newValidator()
	if err != nil {
		return err
	}

	go func() {
		var ctx context.Context
		ctx, s.cancelFull = context.WithCancel(context.Background())
		// 先取消上一次的全量校验
		cancel()
		err := v.Validate(ctx)
		if err != nil {
			s.logger.LogAttrs(ctx, slog.LevelWarn, "退出全量校验", slog.Any("err", err))
		}
	}()
	return nil
}

func (s *Scheduler[T, K]) newValidator() (*validator.Validator[T, K], error) {
	switch s.pattern {
	case connpool.PatternSrcOnly, connpool.PatternSrcFirst:
		return s.srcValidator, nil
	case connpool.PatternDstFirst, connpool.PatternDstOnly:
		return s.dstValidator, nil
	default:
		return nil, fmt.Errorf("未知的 pattern %s", s.pattern)
	}
}
