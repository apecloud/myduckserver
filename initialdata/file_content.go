package initialdata

import _ "embed"

//go:embed pg_class.csv
var pgClassContent string

//go:embed pg_proc.csv
var pgProcContent string

//go:embed pg_type.csv
var pgTypeContent string

var InitialTableDataMap = map[string]string{
	"pg_class": pgClassContent,
	"pg_proc":  pgProcContent,
	"pg_type":  pgTypeContent,
}
