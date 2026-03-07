package tests_test

import (
	"github.com/udugong/migrator"
)

type Order struct {
	Id          int64  `gorm:"primaryKey,autoIncrement"`
	OrderSeq    string `gorm:"uniqueIndex:order_seq;size:255;not null"`
	UpdatedTime int64  `gorm:"index:updated_time,autoUpdateTime:milli"`
	Status      int64
}

func (o Order) UniqueKey() string {
	return o.OrderSeq
}

func (o Order) CompareTo(dst migrator.Entity[string]) bool {
	v, ok := dst.(Order)
	if !ok {
		return false
	}
	o.Id = 0
	v.Id = 0
	return o == v
}

func (o Order) Modify() migrator.Entity[string] {
	o.Id = 0
	return o
}
