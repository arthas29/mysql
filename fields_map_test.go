package mysql

import (
	"context"
	"database/sql"
	"testing"
)

// DemoRow mapping row of demo_table in db
type DemoRow struct {
	FieldKey string  `sql:"field_key"`
	FieldOne string  `sql:"field_one"`
	FieldTwo bool    `sql:"field_two"`
	FieldThr int64   `sql:"field_thr"`
	FieldFou float64 `sql:"field_fou"`
}

var table = "demo_table"

func TestFieldsMap(t *testing.T) {

	obj := DemoRow{
		FieldKey: "field key",
		FieldOne: "field one",
		FieldTwo: true,
		FieldThr: 123,
		FieldFou: 123.45,
	}

	fieldsMap, err := NewFieldsMap(table, &obj)
	if err != nil {
		t.Error(err)
	}

	fields := fieldsMap.GetFields()
	t.Log(fields)

	namesInDB := fieldsMap.GetFieldNamesInDB()
	t.Log(namesInDB)

	addrs := fieldsMap.GetFieldAddrs()
	t.Log(addrs)

	values := fieldsMap.GetFieldValues()
	t.Log(values)

	fieldsStr := fieldsMap.SQLFieldsStr()
	t.Log(fieldsStr)

	fieldsStrForSet := fieldsMap.SQLFieldsStrForSet()
	t.Log(fieldsStrForSet)

	////////////////////////////////////////////////////////////////
	db, err := sql.Open("mysql", "root:123456@/testdb")
	if err != nil {
		t.Log(err.Error())
		return
	}

	ctx := context.Background()

	_, err = fieldsMap.PrepareStmt(ctx, nil, db, " select * from "+table)
	if err != nil {
		t.Log(err.Error())
		return
	}

	testInsert(ctx, db, t, fieldsMap)
	testUpdate(ctx, db, t, fieldsMap)
	testSelectRow(ctx, db, t, fieldsMap)
	testSelectRows(ctx, db, t, fieldsMap)
	testDelete(ctx, db, t, fieldsMap)

	// t.Error("End")
}

////////////////////////////////////////////////////////////////

func testInsert(ctx context.Context, db *sql.DB, t *testing.T, fieldsMap FieldsMap) {

	stmt, err := fieldsMap.SQLInsertStmt(ctx, nil, db)
	if err != nil {
		t.Log(err.Error())
		return
	}
	_, err = stmt.ExecContext(ctx, fieldsMap.GetFieldValues()...)
	if err != nil {
		t.Log(err.Error())
		return
	}
}

func testUpdate(ctx context.Context, db *sql.DB, t *testing.T, fieldsMap FieldsMap) {

	fieldsUp := fieldsMap.GetFields()
	extStr := " where `" + fieldsUp[0].Tag + "` = ? "
	stmt, err := fieldsMap.SQLUpdateStmt(ctx, nil, db, extStr)
	if err != nil {
		t.Log(err.Error())
		return
	}
	fieldsUp = append(fieldsUp, fieldsUp[0])
	_, err = stmt.ExecContext(ctx, fieldsMap.GetFieldValues()...)
	if err != nil {
		t.Log(err.Error())
		return
	}
}

func testDelete(ctx context.Context, db *sql.DB, t *testing.T, fieldsMap FieldsMap) {

	fields := fieldsMap.GetFields()
	extStr := " where `" + fields[0].Tag + "` = ? "
	stmt, err := fieldsMap.SQLDeleteStmt(ctx, nil, db, extStr)
	if err != nil {
		t.Log(err.Error())
		return
	}
	_, err = stmt.ExecContext(ctx, fields[0])
	if err != nil {
		t.Log(err.Error())
		return
	}
}

func testSelectRow(ctx context.Context, db *sql.DB, t *testing.T, fieldsMap FieldsMap) {

	fields := fieldsMap.GetFields()
	extStr := " where `" + fields[0].Tag + "` = ? "
	stmt, err := fieldsMap.SQLSelectStmt(ctx, nil, db, extStr)
	if err != nil {
		t.Log(err.Error())
		return
	}
	r := stmt.QueryRowContext(ctx, fields[0])
	if r == nil {
		t.Log(err.Error())
		return
	}

	err = r.Scan(fieldsMap.GetFieldAddrs()...)
	if err != nil {
		t.Log(err.Error())
		return
	}

	demoRow := *fieldsMap.MappingBackToObject().(*DemoRow)
	t.Log(demoRow)
}

func testSelectRows(ctx context.Context, db *sql.DB, t *testing.T, fieldsMap FieldsMap) {

	fields := fieldsMap.GetFields()
	extStr := " where `" + fields[0].Tag + "` = ? "
	stmt, err := fieldsMap.SQLSelectStmt(ctx, nil, db, extStr)
	if err != nil {
		t.Log(err.Error())
		return
	}
	rs, err := stmt.QueryContext(ctx, fields[0])
	if err != nil {
		t.Log(err.Error())
		return
	}

	var demoRows []DemoRow
	for rs.Next() {
		var demoRow DemoRow
		fieldsMap, err := NewFieldsMap(table, &demoRow)
		if err != nil {
			t.Log(err.Error())
			return
		}

		err = rs.Scan(fieldsMap.GetFieldAddrs()...)
		if err != nil {
			t.Log(err.Error())
			return
		}

		demoRow = *fieldsMap.MappingBackToObject().(*DemoRow)
		demoRows = append(demoRows, demoRow)
	}
}
