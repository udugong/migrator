package migrator

// Entity 模型实体
type Entity[T IntegerOrString] interface {
	// UniqueKey 返回唯一键
	UniqueKey() T
	// CompareTo 对比方法, dst 需与源实体类型一致
	CompareTo(dst Entity[T]) bool
	// Modify 在Fix时可以修改从base查询出来的数据
	Modify() Entity[T]
}

type IntegerOrString interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 | string
}
