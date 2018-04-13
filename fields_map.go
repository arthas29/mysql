package mysql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
)

// Field db field
// describe struct mapping in DB like:
// type DemoRow struct {
// 	FieldKey string  `sql:"field_key"`
// 	FieldOne string  `sql:"field_one"`
// 	FieldTwo bool    `sql:"field_two"`
// 	FieldThr int64   `sql:"field_thr"`
// 	FieldFou float64 `sql:"field_fou"`
// }
//
type Field struct {
	Name        string
	Tag         string
	Type        string
	IntValue    int64
	StringValue string
	FloatValue  float64
	BoolValue   bool
	Addr        interface{}
	Save        []byte // for null string
}

// FieldsMap hold Field
type FieldsMap interface {

	// GetFields Fields
	GetFields() []Field

	// GetFieldNamesInDB get Names in db from Fields
	GetFieldNamesInDB() []string

	// GetFieldValues get Values in Object(struct)
	GetFieldValues() []interface{}

	// GetFieldAddrs get Pointers of Values in Object(struct)
	GetFieldAddrs() []interface{}

	// MappingBackToObject mapping back to the original object
	MappingBackToObject() interface{}

	////////////////////////////////////////////////////////////////
	// generate SQL string
	// SQLFieldsStr generate sqlstr in db from Fields
	SQLFieldsStr() string

	// SQLFieldsStrForSet generate sqlstr in db from Fields for set
	SQLFieldsStrForSet() string

	////////////////////////////////////////////////////////////////
	// generate statement
	// PrepareStmt prepare statement
	PrepareStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
		sqlstr string) (*sql.Stmt, error)

	// SQLSelectStmt generate statement for SELECT
	SQLSelectStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
		extStr string) (*sql.Stmt, error)

	// SQLInsertStmt generate statement for INSERT
	SQLInsertStmt(ctx context.Context, tx *sql.Tx, db *sql.DB) (*sql.Stmt, error)

	// SQLUpdateStmt generate statement for UPDATE
	SQLUpdateStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
		extStr string) (*sql.Stmt, error)

	// SQLDeleteStmt generate statement for DELETE
	SQLDeleteStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
		extStr string) (*sql.Stmt, error)
}

////////////////////////////////////////////////////////////////

// NewFieldsMap new Fields
func NewFieldsMap(table string, objptr interface{}) (FieldsMap, error) {

	elem := reflect.ValueOf(objptr).Elem()
	reftype := elem.Type()

	var fields []Field
	for i, flen := 0, reftype.NumField(); i < flen; i++ {

		var field Field
		field.Name = reftype.Field(i).Name
		field.Tag = reftype.Field(i).Tag.Get("sql")
		field.Type = reftype.Field(i).Type.String()
		field.Addr = elem.Field(i).Addr().Interface()

		switch field.Type {
		case "int64":
			field.IntValue = elem.Field(i).Int()
			break
		case "string":
			field.StringValue = elem.Field(i).String()
			break
		case "float64":
			field.FloatValue = elem.Field(i).Float()
			break
		case "bool":
			field.BoolValue = elem.Field(i).Bool()
			break
		default:
			return nil, errors.New("unsupported field.Type:" + field.Type)
		}

		fields = append(fields, field)
	}

	return &_FieldsMap{
		objptr: objptr,
		fields: fields,
		table:  table,
	}, nil
}

////////////////////////////////////////////////////////////////

var _ FieldsMap = &_FieldsMap{}

type _FieldsMap struct {
	objptr interface{}
	fields []Field
	table  string
}

// GetFields get Fields for an Object(struct)
func (fds *_FieldsMap) GetFields() []Field {

	return fds.fields
}

// GetFieldNamesInDB get Names in db from Fields
// example:
// type DemoRow struct {
// 	FieldKey string  `sql:"field_key"`
// 	FieldOne string  `sql:"field_one"`
// 	FieldTwo bool    `sql:"field_two"`
// 	FieldThr int64   `sql:"field_thr"`
// 	FieldFou float64 `sql:"field_fou"`
// }
//
// return ["field_key", "field_one", "field_two","field_thr","field_fou"]
//
func (fds *_FieldsMap) GetFieldNamesInDB() []string {

	var tags []string
	for i, flen := 0, len(fds.fields); i < flen; i++ {
		tags = append(tags, fds.fields[i].Tag)
	}

	return tags
}

// GetFieldValues get Values in Object(struct)
func (fds *_FieldsMap) GetFieldValues() []interface{} {

	var values []interface{}
	for i, flen := 0, len(fds.fields); i < flen; i++ {
		switch fds.fields[i].Type {
		case "int64":
			values = append(values, fds.fields[i].IntValue)
			break
		case "string":
			values = append(values, fds.fields[i].StringValue)
			break
		case "float64":
			values = append(values, fds.fields[i].FloatValue)
			break
		case "bool":
			values = append(values, fds.fields[i].BoolValue)
			break
		default:
			values = append(values, nil)
			break
		}
	}

	return values
}

// GetFieldAddrs get Pointers of Values in Object(struct)
func (fds *_FieldsMap) GetFieldAddrs() []interface{} {

	var addrs []interface{}
	for i, flen := 0, len(fds.fields); i < flen; i++ {
		if fds.fields[i].Type == "string" {
			// "string" need bytes => string for empty string
			addrs = append(addrs, &fds.fields[i].Save)
		} else {
			addrs = append(addrs, &fds.fields[i].Addr)
		}
	}

	return addrs
}

// MappingBackToObject mapping back to the original object
func (fds *_FieldsMap) MappingBackToObject() interface{} {

	for i, flen := 0, len(fds.fields); i < flen; i++ {
		switch fds.fields[i].Type {
		case "string":
			*fds.fields[i].Addr.(*string) = string(fds.fields[i].Save)
			break
		}
	}

	return fds.objptr
}

////////////////////////////////////////////////////////////////
// generate SQL string

// SQLFieldsStr generate sqlstr in db from Fields
// example:" `field0`, `field1`, `field2`, `field3` "
func (fds *_FieldsMap) SQLFieldsStr() string {

	var tagsStr string
	for i, flen := 0, len(fds.fields); i < flen; i++ {
		if len(tagsStr) > 0 {
			tagsStr += ", "
		}
		tagsStr += "`"
		tagsStr += fds.fields[i].Tag
		tagsStr += "`"
	}
	if len(tagsStr) > 0 {
		tagsStr += " "
		tagsStr = " " + tagsStr
	}

	return tagsStr
}

// SQLFieldsStrForSet generate sqlstr in db from Fields for set
// example:" `field0` = ?, `field1` = ?, `field2` = ?, `field3` = ? "
func (fds *_FieldsMap) SQLFieldsStrForSet() string {

	var tagsStr string
	for i, flen := 0, len(fds.fields); i < flen; i++ {
		if len(tagsStr) > 0 {
			tagsStr += ", "
		}
		tagsStr += "`"
		tagsStr += fds.fields[i].Tag
		tagsStr += "`"
		tagsStr += " = ?"
	}
	if len(tagsStr) > 0 {
		tagsStr += " "
		tagsStr = " " + tagsStr
	}

	return tagsStr
}

////////////////////////////////////////////////////////////////
// generate statement

// PrepareStmt prepare statement
func (fds *_FieldsMap) PrepareStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
	sqlstr string) (*sql.Stmt, error) {

	if tx != nil {
		return tx.PrepareContext(ctx, sqlstr)
	}

	if db != nil {
		return db.PrepareContext(ctx, sqlstr)
	}

	return nil, errors.New("tx & db both nil")
}

// SQLSelectStmt generate statement for SELECT
func (fds *_FieldsMap) SQLSelectStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
	extStr string) (*sql.Stmt, error) {

	sqlstr := "SELECT " + fds.SQLFieldsStr() +
		" FROM `" + fds.table + "` " + extStr

	return fds.PrepareStmt(ctx, tx, db, sqlstr)
}

// SQLInsertStmt generate statement for INSERT
func (fds *_FieldsMap) SQLInsertStmt(ctx context.Context, tx *sql.Tx, db *sql.DB) (*sql.Stmt, error) {

	var vs string
	for i, flen := 0, len(fds.fields); i < flen; i++ {
		if len(vs) > 0 {
			vs += ", "
		}
		vs += "?"
	}

	sqlstr := "INSERT INTO `" + fds.table + "` (" + fds.SQLFieldsStr() + ") " +
		"VALUES (" + vs + ")"
	return fds.PrepareStmt(ctx, tx, db, sqlstr)
}

// SQLUpdateStmt generate statement for UPDATE
func (fds *_FieldsMap) SQLUpdateStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
	extStr string) (*sql.Stmt, error) {

	sqlstr := "UPDATE `" + fds.table + "` SET " + fds.SQLFieldsStrForSet() + extStr
	return fds.PrepareStmt(ctx, tx, db, sqlstr)
}

// SQLDeleteStmt generate statement for DELETE
func (fds *_FieldsMap) SQLDeleteStmt(ctx context.Context, tx *sql.Tx, db *sql.DB,
	extStr string) (*sql.Stmt, error) {

	sqlstr := "DELETE FROM `" + fds.table + "` " + extStr
	return fds.PrepareStmt(ctx, tx, db, sqlstr)
}
