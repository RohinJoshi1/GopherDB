package main

import (
	"fmt"
)

func main() {
	// options := &Options{
	// 	pageSize:       os.Getpagesize(),
	// 	MinFillPercent: 0.0125,
	// 	MaxFillPercent: 0.025,
	// }
	// dal, _ := newDal("./mainTest", options)

	// c := newCollection([]byte("collection1"), dal.root)
	// c.dal = dal

	// _ = c.Put([]byte("Key1"), []byte("Value1"))
	// _ = c.Put([]byte("Key2"), []byte("Value2"))
	// _ = c.Put([]byte("Key3"), []byte("Value3"))
	// _ = c.Put([]byte("Key4"), []byte("Value4"))
	// _ = c.Put([]byte("Key5"), []byte("Value5"))
	// _ = c.Put([]byte("Key6"), []byte("Value6"))
	// item, _ := c.Find([]byte("Key1"))

	// fmt.Printf("key is: %s, value is: %s\n", item.key, item.value)

	// _ = c.Remove([]byte("Key1"))
	// item, _ = c.Find([]byte("Key1"))
	// dal.writeFreelist()
	// fmt.Printf("item = %+v\n", item)
	// _ = dal.close()
	db, _ := Open("Final", &Options{MinFillPercent: 0.5, MaxFillPercent: 1.0})

	tx := db.WriteTx()
	collectionName := "FinalCollection"
	createdCollection, _ := tx.CreateCollection([]byte(collectionName))

	newKey := []byte("key0")
	newVal := []byte("value0")
	_ = createdCollection.Put(newKey, newVal)

	_ = tx.Commit()
	_ = db.Close()

	db, _ = Open("Final", &Options{MinFillPercent: 0.5, MaxFillPercent: 1.0})
	tx = db.ReadTx()
	createdCollection, _ = tx.GetCollection([]byte(collectionName))

	item, _ := createdCollection.Find(newKey)

	_ = tx.Commit()
	_ = db.Close()

	fmt.Printf("key is: %s, value is: %s\n", item.key, item.value)

}
