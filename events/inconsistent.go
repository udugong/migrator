package events

import "github.com/udugong/migrator"

const (
	// InconsistentEventTypeTargetMissing 目标数据中缺失该数据
	InconsistentEventTypeTargetMissing = "target_missing"
	// InconsistentEventTypeNEQ 源数据与目标数据不相等
	InconsistentEventTypeNEQ = "neq"
	// InconsistentEventTypeBaseMissing 源数据中没有该数据
	InconsistentEventTypeBaseMissing = "base_missing"
)

type InconsistentEvent[T migrator.IntegerOrString] struct {
	UniqueKey T
	// Direction 表示修复数据以什么为准
	// SRC: 以源表为准
	// DST: 以目标表为准
	Direction migrator.Direction
	// Type 可选参数
	// 可以用来表示数据不一致的原因
	Type string
}
