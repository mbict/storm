package example

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mbict/storm"
	"log"
	"os"
	"testing"
)

var db storm.Storm

func init() {
	var err error
	db, err = storm.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		panic("cannot create database: " + err.Error())
	}

	logger := log.New(os.Stdout, "[db]", log.Lshortfile|log.Ldate|log.Ltime)
	db.SetLogger(logger)

	must(db.Register(User{}))
	must(db.Register(Item{}))
	must(db.Register(Note{}))
	must(db.Register(Address{}))
	must(db.Register(Tag{}))
	//must(db.Register( OrderTagJunction{} ))
	must(db.Register(Order{}))

	mustExec(db.DB().Exec(`CREATE TABLE 'user' (id int, name varchar)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'item' (id int, name varchar)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'tag' (id int, tag varchar)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'note' (id int, order_id int, message varchar, reported_by_user_id int)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'address' (id int, street varchar, country varchar)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'order' (id int, options varchar, address_id int)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'rel_order_item' (order_id int, item_id int)`))
	mustExec(db.DB().Exec(`CREATE TABLE 'rel_order_tag' (order_id int, tag_id int)`))

	mustExec(db.DB().Exec(`INSERT INTO 'item' (id, NAME) VALUES (1,"item 1"), (2,"item 2"), (3,"item 3"), (4,"item 4")`))
	mustExec(db.DB().Exec(`INSERT INTO 'user' (id, NAME) VALUES (1,"user 1"), (2,"user 2"), (3,"user 3"), (4,"user 4")`))
	mustExec(db.DB().Exec(`INSERT INTO 'address' (id, street, country) VALUES (1,"street 1", "country 1"), (2,"street 2", "country 2")`))
	mustExec(db.DB().Exec(`INSERT INTO 'tag' (id, TAG) VALUES (1,"tag 1"), (2,"tag 2"), (3,"tag 3"), (4,"tag 4")`))
	mustExec(db.DB().Exec(`INSERT INTO 'order' (id, OPTIONS, address_id) VALUES (1,"a", 1), (2,"b",2), (3,"c",1)`))
	mustExec(db.DB().Exec(`INSERT INTO 'note' (id, order_id, MESSAGE, reported_by_user_id) VALUES (1,1,"message 1 order 1", 1), (2,1,"message 2 order 1", 2),(3,2,"message 1 order 2", 4),(3,2,"message 2 order 2", 4),(4,2,"message 3 order 2", 4),(5,2,"message 4 order 2", 1)`))
	mustExec(db.DB().Exec(`INSERT INTO 'rel_order_item' (order_id, item_id) VALUES (1,1),(2,1),(2,2),(3,3),(3,4)`))
	mustExec(db.DB().Exec(`INSERT INTO 'rel_order_tag' (order_id, tag_id) VALUES (1,1),(1,2),(1,3),(2,4)`))
}

func must(err error) {
	if err != nil {
		panic("error: " + err.Error())
	}
}

func mustExec(res sql.Result, err error) sql.Result {
	if err != nil {
		panic("error: " + err.Error())
	}
	return res
}

func TestFindAllOrder(t *testing.T) {
	var orders []*Order
	err := db.FetchRelated("Items", "Notes", "Tags", "Address").Find(&orders)
	if err != nil {
		t.Errorf("did not expected an error but got error: %v", err)
	}

	fmt.Println(orders)
}

func TestFindFirstOrder(t *testing.T) {
	var order *Order
	err := db.FetchRelated("Items", "Notes", "Tags", "Address").Find(&order)
	if err != nil {
		t.Errorf("did not expected an error but got error: %v", err)
	}

	fmt.Println(order)
}

func TestFirstOrder(t *testing.T) {
	var order *Order
	err := db.FetchRelated("Items", "Notes", "Tags", "Address").First(&order)
	if err != nil {
		t.Errorf("did not expected an error but got error: %v", err)
	}

	fmt.Println(order)
}

func TestFindOrderByPK(t *testing.T) {
	var order *Order
	err := db.FetchRelated("Items", "Notes", "Tags", "Address").Find(&order, 3)
	if err != nil {
		t.Errorf("did not expected an error but got error: %v", err)
	}

	fmt.Printf("%#v\n",order)

}
