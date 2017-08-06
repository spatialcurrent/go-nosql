package nosql

type Table struct {
  Name string
  Indexes []string
  ReadUnits int
  WriteUnits int
}
