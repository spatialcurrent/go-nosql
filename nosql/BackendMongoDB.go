package nosql

import (
	"strconv"
)

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type BackendMongoDB struct {
	mongodb_session       *mgo.Session
	mongodb_database_name string
	limit                 int
}

func (b *BackendMongoDB) Type() string {
	return "mongodb"
}

func (b *BackendMongoDB) Connect(options map[string]string) error {
	mongodb_session, err := mgo.Dial(options["DatabaseUri"])
	if err != nil {
		return err
	}
	b.mongodb_session = mongodb_session
	b.mongodb_database_name = options["DatabaseName"]
	if limit, err := strconv.Atoi(options["Limit"]); err == nil {
		b.limit = limit
	} else {
		b.limit = 1000
	}

	return nil
}

func (b *BackendMongoDB) GetCollection(collection_name string) *mgo.Collection {
	return b.mongodb_session.DB(b.mongodb_database_name).C(collection_name)
}

func (b *BackendMongoDB) GetItemById(table_name string, id string, item interface{}) error {
	c := b.GetCollection(table_name)
	err := c.Find(bson.M{"_id": id}).One(item)
	return err
}

func (b *BackendMongoDB) GetItemsByIds(table_name string, ids []string, sort_fields []string, items interface{}) error {
	c := b.GetCollection(table_name)
	var iter *mgo.Iter
	if len(sort_fields) > 0 {
		iter = c.Find(bson.M{"_id": bson.M{"$in": ids}}).Sort(sort_fields...).Iter()
	} else {
		iter = c.Find(bson.M{"_id": bson.M{"$in": ids}}).Iter()
	}
	err := iter.All(items)
	return err
}

func (b *BackendMongoDB) GetItemByAttributeValue(table_name string, attribute_name string, attribute_value string, item interface{}) error {
	c := b.GetCollection(table_name)
	q := bson.M{}
	q[attribute_name] = attribute_value
	err := c.Find(q).One(item)
	return err
}

func (b *BackendMongoDB) GetItemsByAttributeValue(table_name string, attribute_name string, attribute_value string, sort_fields []string, items interface{}) error {
	c := b.GetCollection(table_name)
	q := bson.M{}
	q[attribute_name] = attribute_value
	var iter *mgo.Iter
	if len(sort_fields) > 0 {
		iter = c.Find(q).Sort(sort_fields...).Limit(b.limit).Iter()
	} else {
		iter = c.Find(q).Limit(b.limit).Iter()
	}
	err := iter.All(items)
	return err
}

func (b *BackendMongoDB) GetItems(table_name string, index_name string, sort_fields []string, items interface{}) error {
	c := b.GetCollection(table_name)
	var iter *mgo.Iter
	if len(sort_fields) > 0 {
		iter = c.Find(nil).Sort(sort_fields...).Limit(b.limit).Iter()
	} else {
		iter = c.Find(nil).Limit(b.limit).Iter()
	}
	err := iter.All(items)
	return err
}

func (b *BackendMongoDB) RemoveItemById(table_name string, id string) error {
	c := b.GetCollection(table_name)
	c.Remove(bson.M{"_id": id})
	return nil
}

func (b *BackendMongoDB) RemoveItemByAttributeValue(table_name string, attribute_name string, attribute_value string) error {
	c := b.GetCollection(table_name)
	q := bson.M{}
	q[attribute_name] = attribute_value
	c.Remove(q)
	return nil
}

func (b *BackendMongoDB) RemoveItemsByAttributeValue(table_name string, attribute_name string, attribute_value string) error {
	c := b.GetCollection(table_name)
	q := bson.M{}
	q[attribute_name] = attribute_value
	c.Remove(q)
	return nil
}

func (b *BackendMongoDB) RemoveAll(table_name string) error {
	c := b.GetCollection(table_name)
	_, err := c.RemoveAll(nil)
	return err
}

func (b *BackendMongoDB) InsertItem(table_name string, item interface{}) error {
	c := b.GetCollection(table_name)
	err := c.Insert(item)
	items := make([]bson.M, 0)
	iter := c.Find(nil).Limit(b.limit).Iter()
	err = iter.All(&items)
	return err
}

func (b *BackendMongoDB) UpdateItemById(table_name string, id string, values map[string]interface{}) error {
	c := b.GetCollection(table_name)
	u := bson.M{}
	for k, v := range values {
		u[k] = v
	}
	c.Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{"$set": u})
	return nil
}

func (b *BackendMongoDB) CreateTables(tables []Table) error {
	for _, t := range tables {
		b.CreateTable(t.Name, t.Indexes, t.ReadUnits, t.WriteUnits)
	}
	return nil
}

func (b *BackendMongoDB) CreateTable(table_name string, indexes []string, readUnits int, writeUnits int) error {
	// MongoDB tables are automatically created when adding the first item.
	return nil
}

func (b *BackendMongoDB) DeleteTables(table_names []string) error {
	for _, table_name := range table_names {
		b.DeleteTable(table_name)
	}
	return nil
}

func (b *BackendMongoDB) DeleteTable(table_name string) error {
	c := b.GetCollection(table_name)
	err := c.DropCollection()
	return err
}
