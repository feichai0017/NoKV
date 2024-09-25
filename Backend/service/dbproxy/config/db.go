package config

import "fmt"

var (
	TiDBSource = "root:@tcp(localhost:4000)/fileserver?charset=utf8"
)

func UpdateDBHost(host string) {
	TiDBSource = fmt.Sprintf("root:@tcp(%s:4000)/fileserver?charset=utf8", host)
	fmt.Println("Updated TiDBSource:", TiDBSource) // Debug log
}
