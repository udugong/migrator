package tests_test

import (
	"log/slog"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/udugong/gormx/connpool"
	"github.com/udugong/migrator"
	"github.com/udugong/migrator/events"
	evtfixer "github.com/udugong/migrator/events/fixer"
	"github.com/udugong/migrator/fixer"
	"github.com/udugong/migrator/scheduler"
	"github.com/udugong/migrator/validator"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func InitLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

type SrcDB *gorm.DB
type DstDB *gorm.DB

func InitSrcDB() (SrcDB, error) {
	dsn := os.Getenv("SRC_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(localhost:13306)/migrator_test"
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db.Debug(), nil
}

func InitDstDB() (DstDB, error) {
	dsn := os.Getenv("DST_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(localhost:13306)/migrator_new_test"
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db.Debug(), nil
}

func InitDoubleWritePool(src SrcDB, dst DstDB, logger *slog.Logger) *connpool.DoubleWritePool {
	return connpool.NewDoubleWritePool(src, dst, logger)
}

func InitBizDB(p *connpool.DoubleWritePool) *gorm.DB {
	doubleWrite, err := gorm.Open(mysql.New(mysql.Config{
		Conn: p,
	}))
	if err != nil {
		panic(err)
	}
	return doubleWrite
}

func InitKafkaReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9094"},
		Topic:   "inconsistent_order",
		GroupID: "inconsistent_order_fixer",
	})
}

func InitKafkaWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP("localhost:9094"),
		Topic:        "inconsistent_order",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, // ack模式
		Async:        true,             // 异步
	}
}

func InitOrderProducer(writer *kafka.Writer) events.Producer[string] {
	return events.NewKafkaGoProducer[string](writer)
}

func InitFixerConsumer(reader *kafka.Reader, src *gorm.DB, dst *gorm.DB, logger *slog.Logger) (*evtfixer.Consumer[Order, string], error) {
	srcFirst, err := fixer.NewOverrideFixer(src, dst, logger,
		fixer.WithUniqueColumnName[Order, string]("order_seq"),
		fixer.WithUpsertColumns[Order, string]([]string{
			"updated_time",
			"status",
		}),
	)
	if err != nil {
		return nil, err
	}
	dstFirst, err := fixer.NewOverrideFixer(dst, src, logger,
		fixer.WithUniqueColumnName[Order, string]("order_seq"),
		fixer.WithUpsertColumns[Order, string]([]string{
			"updated_time",
			"status",
		}),
	)
	if err != nil {
		return nil, err
	}
	return evtfixer.NewConsumer[Order, string](reader, srcFirst, dstFirst, logger)
}

func InitScheduler(logger *slog.Logger, src, dst *gorm.DB, pool *connpool.DoubleWritePool,
	producer events.Producer[string]) *scheduler.Scheduler[Order, string] {
	srcValidator := validator.NewValidator[Order, string](src, dst, migrator.SRC, producer, logger).
		BatchSize(2).UniqueColumnName("order_seq")
	dstValidator := validator.NewValidator[Order, string](dst, src, migrator.DST, producer, logger).
		BatchSize(2).UniqueColumnName("order_seq")
	return scheduler.NewScheduler[Order, string](srcValidator, dstValidator, pool, logger)
}
