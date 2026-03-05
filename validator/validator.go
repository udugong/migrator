package validator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"github.com/udugong/migrator"
	"github.com/udugong/migrator/events"
)

type Validator[T migrator.Entity[K], K migrator.IntegerOrString] struct {
	baseValidator[K]
	batchSize   int
	updatedTime int64
	// sleepInterval 校验时无数据的睡眠间隔
	// sleepInterval > 0 休眠一段时间
	// sleepInterval <= 0 结束当次循环
	sleepInterval         time.Duration
	uniqueColumnName      string // 唯一键的列名默认 Id
	updatedTimeColumnName string // 更新时间的列名默认 updated_time
}

func NewValidator[T migrator.Entity[K], K migrator.IntegerOrString](base *gorm.DB, target *gorm.DB,
	direction migrator.Direction, producer events.Producer[K], logger *slog.Logger) *Validator[T, K] {
	return &Validator[T, K]{
		baseValidator: baseValidator[K]{
			base:      base,
			target:    target,
			direction: direction,
			producer:  producer,
			logger:    logger,
		},
		batchSize:   100,
		updatedTime: 0,
		// 默认是全量校验，并且数据没了就结束
		sleepInterval:         0,
		uniqueColumnName:      "Id",
		updatedTimeColumnName: "updated_time",
	}
}

func (v *Validator[T, K]) BatchSize(batchSize int) *Validator[T, K] {
	v.batchSize = batchSize
	return v
}

func (v *Validator[T, K]) UpdatedTime(updatedTime int64) *Validator[T, K] {
	v.updatedTime = updatedTime
	return v
}

func (v *Validator[T, K]) SleepInterval(i time.Duration) *Validator[T, K] {
	v.sleepInterval = i
	return v
}

func (v *Validator[T, K]) UniqueColumnName(uniqueColumnName string) *Validator[T, K] {
	v.uniqueColumnName = uniqueColumnName
	return v
}

func (v *Validator[T, K]) UpdatedTimeColumnName(updatedTimeColumnName string) *Validator[T, K] {
	v.updatedTimeColumnName = updatedTimeColumnName
	return v
}

// Validate 执行校验
func (v *Validator[T, K]) Validate(ctx context.Context) error {
	var eg errgroup.Group
	eg.Go(func() error {
		return v.baseToTarget(ctx)
	})
	eg.Go(func() error {
		return v.targetToBase(ctx)
	})
	return eg.Wait()
}

// baseToTarget 从 base 到 target 的验证
func (v *Validator[T, K]) baseToTarget(ctx context.Context) error {
	offset := 0
	for {
		var srcs []T
		dbCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := v.base.WithContext(dbCtx).Model(new(T)).
			Order(v.uniqueColumnName).
			Where(v.updatedTimeColumnName+" >= ?", v.updatedTime).
			Offset(offset).Limit(v.batchSize).Find(&srcs).Error
		cancel()

		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return err
		case err == nil && len(srcs) == 0:
			// 结束，没有数据
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
			continue
		case err == nil:
			diffCtx, diffCancel := context.WithTimeout(ctx, time.Second)
			err := v.dstDiff(diffCtx, srcs)
			diffCancel()
			if err != nil {
				v.logger.LogAttrs(diffCtx, slog.LevelError, "dstDiff 对比失败", slog.Any("err", err))
			}
		default:
			v.logger.LogAttrs(dbCtx, slog.LevelError, "src => dst 查询源表失败", slog.Any("err", err))
		}
		if len(srcs) < v.batchSize {
			// 结束，没有数据
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
			continue
		}
		offset += len(srcs)
	}
}

func (v *Validator[T, K]) dstDiff(ctx context.Context, srcs []T) error {
	uniqueKeys := make([]K, 0, len(srcs))
	for _, src := range srcs {
		uniqueKeys = append(uniqueKeys, src.UniqueKey())
	}

	var dsts []T
	err := v.target.WithContext(ctx).Model(new(T)).Where(v.uniqueColumnName+" IN ?", uniqueKeys).
		Find(&dsts).Error
	if err != nil {
		return err
	}
	dstMap := v.toMap(dsts)
	for _, src := range srcs {
		dst, ok := dstMap[src.UniqueKey()]
		// 目标表缺失数据
		if !ok {
			v.notify(src.UniqueKey(), events.InconsistentEventTypeTargetMissing)
			continue
		}
		// 源表数据与目标表数据不同
		if !src.CompareTo(dst) {
			v.notify(src.UniqueKey(), events.InconsistentEventTypeNEQ)
			continue
		}
	}
	return nil
}

func (v *Validator[T, K]) toMap(data []T) map[K]T {
	res := make(map[K]T, len(data))
	for _, val := range data {
		res[val.UniqueKey()] = val
	}
	return res
}

// targetToBase 执行 target 到 base 的验证
// 可以找出 dst 中多余的数据
func (v *Validator[T, K]) targetToBase(ctx context.Context) error {
	offset := 0
	for {
		var ts []T
		dbCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := v.target.WithContext(dbCtx).Model(new(T)).Select(v.uniqueColumnName).Offset(offset).
			Limit(v.batchSize).Find(&ts).Error
		cancel()

		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil
		case err == nil && len(ts) == 0:
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
			continue
		case err == nil:
			v.srcMissingRecords(ctx, ts)
		default:
			v.logger.LogAttrs(dbCtx, slog.LevelError, "dst => src 查询目标表失败", slog.Any("err", err))
		}
		if len(ts) < v.batchSize {
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
			continue
		}
		offset += v.batchSize
	}
}

func (v *Validator[T, K]) srcMissingRecords(ctx context.Context, ts []T) {
	if len(ts) == 0 {
		return
	}
	uniqueKeys := make([]K, 0, len(ts))
	for _, t := range ts {
		uniqueKeys = append(uniqueKeys, t.UniqueKey())
	}

	var srcTs []T
	dbCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := v.base.WithContext(dbCtx).Model(new(T)).Select(v.uniqueColumnName).
		Where(v.uniqueColumnName+" IN ?", uniqueKeys).Find(&srcTs).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound), len(srcTs) == 0:
		// 说明 uniqueKeys 全部没有
		v.batchNotify(uniqueKeys, events.InconsistentEventTypeBaseMissing)
	case err == nil:
		// 计算差集
		v.batchNotify(v.difference(ts, srcTs), events.InconsistentEventTypeBaseMissing)
	default:
		v.logger.LogAttrs(dbCtx, slog.LevelError, "dst => src 查询源表失败", slog.Any("err", err))
	}
}

func (v *Validator[T, K]) difference(src, dst []T) []K {
	length := len(src)
	if len(dst) > length {
		length = len(dst)
	}
	dstSet := make(map[K]struct{}, length)
	for _, v := range dst {
		dstSet[v.UniqueKey()] = struct{}{}
	}
	diff := make([]K, 0, length)
	for _, val := range src {
		if _, ok := dstSet[val.UniqueKey()]; !ok {
			diff = append(diff, val.UniqueKey())
		}
	}
	return diff
}
