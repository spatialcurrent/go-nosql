package nosql

type Backend interface {
	Type() string
	Connect(map[string]string) error
	CreateTables(tables []Table) error
	CreateTable(table_name string, indexes []string, readUnits int, writeUnits int) error
	DeleteTables(table_names []string) error
	DeleteTable(table_name string) error
	GetItems(table_name string, sort_fields []string, item interface{}) error
	GetItemById(table_name string, id string, item interface{}) error
	GetItemsByIds(table_name string, ids []string, sort_fields []string, items interface{}) error
	GetItemByAttributeValue(table_name string, attribute_name string, attribute_value string, item interface{}) error
	GetItemsByAttributeValue(table_name string, attribute_name string, attribute_value string, sort_fields []string, items interface{}) error
	InsertItem(table_name string, item interface{}) error
	UpdateItemById(table_name string, id string, item map[string]interface{}) error
	RemoveItemById(table_name string, id string) error
	RemoveItemByAttributeValue(table_name string, attribute_name string, attribute_value string) error
	RemoveItemsByAttributeValue(table_name string, attribute_name string, attribute_value string) error
	RemoveAll(table_name string) error
}
