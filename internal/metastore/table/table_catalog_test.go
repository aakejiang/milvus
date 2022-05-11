package table

import (
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func connect() (*sqlx.DB, error) {
	return sqlx.Connect(
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s",
			"root",
			"11111111",
			"localhost",
			"3306",
			"milvus_meta",
		),
	)
}

type Hoge struct {
	ID  int    `db:"id"`
	Foo string `db:"foo"`
}

type Fuga struct {
	ID   int    `db:"id"`
	Hoge Hoge   `db:"hoge"`
	Bar  string `db:"bar"`
}

func TestJoin(t *testing.T) {
	db, err := connect()
	if err != nil {
		panic(err)
	}

	s := "SELECT h.id as \"hoge.id\", h.foo AS \"hoge.foo\", f.id as id, f.bar as bar FROM fuga AS f JOIN hoge AS h ON f.hoge_id = h.id"
	var hs []Fuga
	err = db.Select(&hs, s)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", hs)
}
