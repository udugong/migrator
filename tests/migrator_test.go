package tests_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/udugong/migrator/scheduler"
	"gorm.io/gorm"
)

type MigratorTestSuite struct {
	suite.Suite
	db        *gorm.DB
	srcDB     *gorm.DB
	dstDB     *gorm.DB
	scheduler *scheduler.Scheduler[Order, string]
}

func (s *MigratorTestSuite) SetupSuite() {
	t := s.T()
	logger := InitLogger()
	srcDB, err := InitSrcDB()
	require.NoError(t, err)
	dstDB, err := InitDstDB()
	require.NoError(t, err)
	pool := InitDoubleWritePool(srcDB, dstDB, logger)
	s.db = InitBizDB(pool)
	s.srcDB = srcDB
	s.dstDB = dstDB
	require.NoError(t, s.srcDB.AutoMigrate(&Order{}))
	require.NoError(t, s.dstDB.AutoMigrate(&Order{}))
	producer := InitOrderProducer(InitKafkaWriter())
	s.scheduler = InitScheduler(logger, srcDB, dstDB, pool, producer)
	consumer, err := InitFixerConsumer(InitKafkaReader(), srcDB, dstDB, logger)
	require.NoError(t, err)
	require.NoError(t, consumer.Start())
}

func (s *MigratorTestSuite) SetupTest() {
	t := s.T()
	require.NoError(t, s.insertData(s.srcDB, s.generateData()))
}

func (s *MigratorTestSuite) TearDownTest() {
	s.srcDB.Exec("TRUNCATE TABLE orders")
	s.dstDB.Exec("TRUNCATE TABLE orders")
}

func (s *MigratorTestSuite) TestSrcOnly() {
	t := s.T()
	require.NoError(t, s.scheduler.SrcOnly())
	baseOrder := Order{
		Id:          4,
		OrderSeq:    "210004",
		UpdatedTime: 1772698297378,
		Status:      0,
	}
	err := s.db.Model(&Order{}).Create(&baseOrder).Error
	require.NoError(t, err)
	var srcData Order
	err = s.srcDB.Model(&Order{}).Where(Order{OrderSeq: "210004"}).First(&srcData).Error
	require.NoError(t, err)
	assert.Equal(t, baseOrder, srcData)
	var dstData Order
	err = s.dstDB.Model(&Order{}).Where(Order{OrderSeq: "210004"}).First(&dstData).Error
	assert.Equal(t, gorm.ErrRecordNotFound, err)
}

func (s *MigratorTestSuite) TestSrcFirst() {
	t := s.T()
	require.NoError(t, s.scheduler.SrcFirst())
	baseOrder := Order{
		Id:          5,
		OrderSeq:    "210005",
		UpdatedTime: 1772698297379,
		Status:      0,
	}
	err := s.db.Model(&Order{}).Create(baseOrder).Error
	require.NoError(t, err)
	var srcData Order
	err = s.srcDB.Model(&Order{}).Where(Order{OrderSeq: "210005"}).First(&srcData).Error
	require.NoError(t, err)
	assert.Equal(t, baseOrder, srcData)
	var dstData Order
	err = s.dstDB.Model(&Order{}).Where(Order{OrderSeq: "210005"}).First(&dstData).Error
	require.NoError(t, err)
	assert.Equal(t, baseOrder, dstData)
}

func (s *MigratorTestSuite) TestDstFirst() {
	t := s.T()
	require.NoError(t, s.scheduler.DstFirst())
	baseOrder := Order{
		Id:          6,
		OrderSeq:    "210006",
		UpdatedTime: 1772698297380,
		Status:      0,
	}
	err := s.db.Model(&Order{}).Create(baseOrder).Error
	require.NoError(t, err)
	var srcData Order
	err = s.srcDB.Model(&Order{}).Where(Order{OrderSeq: "210006"}).First(&srcData).Error
	require.NoError(t, err)
	assert.Equal(t, baseOrder, srcData)
	var dstData Order
	err = s.dstDB.Model(&Order{}).Where(Order{OrderSeq: "210006"}).First(&dstData).Error
	require.NoError(t, err)
	assert.Equal(t, baseOrder, dstData)
}

func (s *MigratorTestSuite) TestDstOnly() {
	t := s.T()
	require.NoError(t, s.scheduler.DstOnly())
	baseOrder := Order{
		Id:          7,
		OrderSeq:    "210007",
		UpdatedTime: 1772698297381,
		Status:      0,
	}
	err := s.db.Model(&Order{}).Create(baseOrder).Error
	require.NoError(t, err)
	var dstData Order
	err = s.dstDB.Model(&Order{}).Where(Order{OrderSeq: "210007"}).First(&dstData).Error
	require.NoError(t, err)
	assert.Equal(t, baseOrder, dstData)
	err = s.srcDB.Model(&Order{}).Where(Order{OrderSeq: "210007"}).First(&dstData).Error
	assert.Equal(t, gorm.ErrRecordNotFound, err)
}

func (s *MigratorTestSuite) TestStartFullValidationBySrcFirst() {
	t := s.T()
	require.NoError(t, s.scheduler.SrcFirst())
	dstData := make([]Order, 0, 3)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	require.Equal(t, 0, len(dstData))
	// 启动全量校验
	require.NoError(t, s.scheduler.StartFullValidation())
	time.Sleep(30 * time.Second)
	// 全量校验结束后插入数据
	err := s.srcDB.Model(&Order{}).Create(&Order{OrderSeq: "210009", UpdatedTime: 1772698297383}).Error
	require.NoError(t, err)
	time.Sleep(30 * time.Second)
	// 对比数据,已源表为准
	err = s.dstDB.Model(&Order{}).Order("order_seq").Find(&dstData).Error
	require.NoError(t, err)
	assert.Equal(t, 3, len(dstData))
	srcData := make([]Order, 0, 4)
	err = s.srcDB.Model(&Order{}).Order("order_seq").Find(&srcData).Error
	require.NoError(t, err)
	for idx, data := range dstData {
		assert.Equal(t, srcData[idx], data)
	}
}

func (s *MigratorTestSuite) TestStartFullValidationByDstFirst() {
	t := s.T()
	require.NoError(t, s.scheduler.DstFirst())
	require.NoError(t, s.insertData(s.dstDB, s.generateData()))
	res := s.dstDB.Model(&Order{}).Where("order_seq = ?", "210002").Updates(&Order{Status: 1})
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected)
	res = s.dstDB.Model(&Order{}).Where("order_seq = ?", "210001").Delete(&Order{})
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected)
	// 启动全量校验
	require.NoError(t, s.scheduler.StartFullValidation())
	time.Sleep(30 * time.Second)
	// 对比数据,以目标表为准
	dstData := make([]Order, 0, 2)
	require.NoError(t, s.dstDB.Model(&Order{}).Order("order_seq").Find(&dstData).Error)
	assert.Equal(t, 2, len(dstData))
	srcData := make([]Order, 0, 2)
	require.NoError(t, s.srcDB.Model(&Order{}).Order("order_seq").Find(&srcData).Error)
	for idx, data := range dstData {
		assert.Equal(t, srcData[idx], data)
	}
}

func (s *MigratorTestSuite) TestStopFullValidation() {
	t := s.T()
	require.NoError(t, s.scheduler.SrcFirst())
	dstData := make([]Order, 0, 3)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	require.Equal(t, 0, len(dstData))
	require.NoError(t, s.scheduler.StartFullValidation())
	time.Sleep(time.Millisecond)
	s.scheduler.StopFullValidation()
	time.Sleep(30 * time.Second)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	assert.Equal(t, 0, len(dstData))
}

func (s *MigratorTestSuite) TestStartIncrementValidation() {
	t := s.T()
	require.NoError(t, s.scheduler.SrcFirst())
	dstData := make([]Order, 0, 3)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	require.Equal(t, 0, len(dstData))
	require.NoError(t, s.scheduler.StartIncrementValidation(1772698297376, 3000))
	time.Sleep(30 * time.Second)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	time.Sleep(30 * time.Second)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	assert.Equal(t, 2, len(dstData))
	srcOrders := s.generateData()
	for idx, data := range dstData {
		assert.Equal(t, true, data.CompareTo(srcOrders[idx+1]))
	}
	res := s.srcDB.Model(&Order{}).Create(&Order{OrderSeq: "210010", UpdatedTime: 1772698297384, Status: 1})
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected)
	time.Sleep(30 * time.Second)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	assert.Equal(t, 3, len(dstData))
	srcData := make([]Order, 0, 4)
	require.NoError(t, s.srcDB.Model(&Order{}).Find(&srcData).Error)
	for idx, data := range dstData {
		assert.Equal(t, true, data.CompareTo(srcData[idx+1]))
	}
}

func (s *MigratorTestSuite) TestStopIncrementValidation() {
	t := s.T()
	require.NoError(t, s.scheduler.SrcFirst())
	dstData := make([]Order, 0, 3)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	require.Equal(t, 0, len(dstData))
	require.NoError(t, s.scheduler.StartIncrementValidation(1772698297376, 3000))
	time.Sleep(time.Millisecond)
	s.scheduler.StopIncrementValidation()
	time.Sleep(30 * time.Second)
	require.NoError(t, s.dstDB.Model(&Order{}).Find(&dstData).Error)
	assert.Equal(t, 0, len(dstData))
}

func (*MigratorTestSuite) generateData() []Order {
	return []Order{
		{
			Id:          1,
			OrderSeq:    "210001",
			UpdatedTime: 1772698297375,
			Status:      0,
		},
		{
			Id:          2,
			OrderSeq:    "210002",
			UpdatedTime: 1772698297376,
			Status:      0,
		},
		{
			Id:          3,
			OrderSeq:    "210003",
			UpdatedTime: 1772698297377,
			Status:      0,
		},
	}
}

func (s *MigratorTestSuite) insertData(db *gorm.DB, orders []Order) error {
	return db.Model(&Order{}).Create(orders).Error
}

func TestMigrator(t *testing.T) {
	suite.Run(t, new(MigratorTestSuite))
}
