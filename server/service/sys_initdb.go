package service

import (
	"database/sql"
	"errors"
	"fmt"
	"gin-vue-admin/config"
	"gin-vue-admin/global"
	"gin-vue-admin/model"
	"gin-vue-admin/model/request"
	"gin-vue-admin/source"
	"gin-vue-admin/utils"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"path/filepath"
	_ "github.com/lib/pq"
)

//@author: [songzhibin97](https://github.com/songzhibin97)
//@function: writeConfig
//@description: 回写配置
//@param:
//@return: error

func writeConfig(viper *viper.Viper, db interface{}) error {
	switch global.GVA_CONFIG.System.DbType {
	case "mysql":
		global.GVA_CONFIG.Mysql = db.(config.Mysql)
	case "postgres":
		global.GVA_CONFIG.Postgresql = db.(config.Postgresql)
	default:
		global.GVA_CONFIG.Mysql = db.(config.Mysql)
	}
	cs := utils.StructToMap(global.GVA_CONFIG)
	for k, v := range cs {
		viper.Set(k, v)
	}
	return viper.WriteConfig()
}

//@author: [songzhibin97](https://github.com/songzhibin97)
//@function: createTable
//@description: 创建数据库(mysql)
//@param: dsn string, driver string, createSql
//@return: error

func createTable(dsn string, driver string, createSql string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return err
	}
	_, err = db.Exec(createSql)
	return err
}

func initDB(InitDBFunctions ...model.InitDBFunc) (err error) {
	for _, v := range InitDBFunctions {
		err = v.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

//@author: [songzhibin97](https://github.com/songzhibin97)
//@function: InitDB
//@description: 创建数据库并初始化
//@param: authorityId string
//@return: err error, treeMap map[string][]model.SysMenu

func InitDB(conf request.InitDB) (err error) {
	var configDb interface{}
	switch conf.SqlType {
	case "mysql":
		configDb, err = createDBMysql(conf)
	case "postgres":
		configDb, err = createDBPgsql(conf)
	default:
		configDb, err = createDBMysql(conf)
	}
	if err != nil {
		return err
	}
	//自动迁移
	err = global.GVA_DB.AutoMigrate(
		model.SysUser{},
		model.SysAuthority{},
		model.SysApi{},
		model.SysBaseMenu{},
		model.SysBaseMenuParameter{},
		model.JwtBlacklist{},
		model.SysDictionary{},
		model.SysDictionaryDetail{},
		model.ExaFileUploadAndDownload{},
		model.ExaFile{},
		model.ExaFileChunk{},
		model.ExaSimpleUploader{},
		model.ExaCustomer{},
		model.SysOperationRecord{},
	)
	if err != nil {
		return err
	}
	err = initDB(
		source.Admin,
		source.Api,
		source.AuthorityMenu,
		source.Authority,
		source.AuthoritiesMenus,
		source.Casbin,
		source.DataAuthorities,
		source.Dictionary,
		source.DictionaryDetail,
		source.File,
		source.BaseMenu)
	if err != nil {
		_ = writeConfig(global.GVA_VP, configDb)
		return err
	}
	global.GVA_CONFIG.AutoCode.Root, _ = filepath.Abs("..")
	return nil
}

//初始化数据库和修改配置文件 MYSQL
func createDBMysql(conf request.InitDB) (interface{}, error) {
	if conf.Host == "" {
		conf.Host = "127.0.0.1"
	}

	if conf.Port == "" {
		conf.Port = "3306"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", conf.UserName, conf.Password, conf.Host, conf.Port)
	createSql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci;", conf.DBName)
	if err := createTable(dsn, "mysql", createSql); err != nil {
		return nil, err
	}

	MysqlConfig := config.Mysql{
		Path:     fmt.Sprintf("%s:%s", conf.Host, conf.Port),
		Dbname:   conf.DBName,
		Username: conf.UserName,
		Password: conf.Password,
		Config:   "charset=utf8mb4&parseTime=True&loc=Local",
	}
	global.GVA_CONFIG.System.DbType = conf.SqlType
	if err := writeConfig(global.GVA_VP, MysqlConfig); err != nil {
		return nil, err
	}
	m := global.GVA_CONFIG.Mysql
	if m.Dbname == "" {
		return nil, errors.New("Dbname不得为空")
	}
	linkDns := m.Dsn()
	mysqlConfig := mysql.Config{
		DSN:                       linkDns, // DSN data source name
		DefaultStringSize:         191,     // string 类型字段的默认长度
		DisableDatetimePrecision:  true,    // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,    // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,    // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false,   // 根据版本自动配置
	}
	if db, err := gorm.Open(mysql.New(mysqlConfig), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true}); err != nil {
		//global.GVA_LOG.Error("MySQL启动异常", zap.Any("err", err))
		//os.Exit(0)
		//return nil
		return nil, err
	} else {
		sqlDB, _ := db.DB()
		sqlDB.SetMaxIdleConns(m.MaxIdleConns)
		sqlDB.SetMaxOpenConns(m.MaxOpenConns)
		global.GVA_DB = db
		return MysqlConfig, nil
	}
}

//初始化数据库和修改配置文件 PGSQL
func createDBPgsql(conf request.InitDB) (interface{}, error) {
	if conf.Host == "" {
		conf.Host = "127.0.0.1"
	}

	if conf.Port == "" {
		conf.Port = "3306"
	}
	dsn := "host=" + conf.Host + " user=" + conf.UserName + " password=" + conf.Password + " port=" + conf.Port + " sslmode=disable TimeZone=Asia/Shanghai"
	createSql := fmt.Sprintf("CREATE DATABASE %s ;", conf.DBName)
	if err := createTable(dsn, "postgres", createSql); err != nil {
		return nil, err
	}
	PgsqlConfig := config.Postgresql{
		Host:     conf.Host,
		Port:     conf.Port,
		Dbname:   conf.DBName,
		Username: conf.UserName,
		Password: conf.Password,
		Config:   "sslmode=disable TimeZone=Asia/Shanghai",
	}
	global.GVA_CONFIG.System.DbType = conf.SqlType
	if err := writeConfig(global.GVA_VP, PgsqlConfig); err != nil {
		return nil, err
	}
	m := global.GVA_CONFIG.Postgresql
	if m.Dbname == "" {
		return nil, errors.New("Dbname不得为空")
	}
	linkDns := m.Dsn()
	postgresConfig := postgres.Config{
		DSN:                  linkDns,                // DSN data source name
		PreferSimpleProtocol: m.PreferSimpleProtocol, // 禁用隐式 prepared statement
	}
	if db, err := gorm.Open(postgres.New(postgresConfig), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true}); err != nil {
		//global.GVA_LOG.Error("PgSQL启动异常", zap.Any("err", err))
		//os.Exit(0)
		//return nil
		return nil, err
	} else {

		sqlDB, _ := db.DB()
		sqlDB.SetMaxIdleConns(m.MaxIdleConns)
		sqlDB.SetMaxOpenConns(m.MaxOpenConns)
		global.GVA_DB = db
		return PgsqlConfig, nil
	}
}
