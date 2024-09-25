package tidb

import (
	cfg "cloud_distributed_storage/Backend/service/dbproxy/config"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func InitDBConn() error {
	var err error
	db, err = sql.Open("mysql", cfg.TiDBSource)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	db.SetMaxOpenConns(1000)
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to TiDB: %v", err)
	}

	log.Println("Successfully connected to TiDB database")
	return nil
}

// DBConn 返回数据库连接对象
func DBConn() *sql.DB {
	return db
}

// ParseRows 将查询结果解析为 map
func ParseRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for j := range values {
		scanArgs[j] = &values[j]
	}

	var result []map[string]interface{}
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		entry := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				entry[columns[i]] = col
			}
		}
		result = append(result, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}
