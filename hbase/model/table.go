package model

type TableDescriptor struct {
	table string

	// key is family name
	// value is family option (map[option name]option value), if you want use default option, set value nil.
	families map[string]map[string]string
}

var defaultAttributes = map[string]string{
	"DATA_BLOCK_ENCODING": "NONE",
	"COMPRESSION":         "SNAPPY",
}

func NewTableDescriptor(tablename string, families ...string) *TableDescriptor {
	familiesMap := make(map[string]map[string]string)
	for _, family := range families {
		familiesMap[family] = defaultAttributes
	}
	return &TableDescriptor{
		table:    tablename,
		families: familiesMap,
	}
}

func (td *TableDescriptor) GetTable() string {
	return td.table
}

func (td *TableDescriptor) GetFamilies() map[string]map[string]string {
	return td.families
}
