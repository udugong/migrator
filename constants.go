package migrator

// Direction 修复方向
// 表示修复数据以什么为准
type Direction string

const (
	SRC Direction = "SRC" // 以源表为准
	DST Direction = "DST" // 以目标表为准
)
