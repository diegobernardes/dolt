// Copyright 2020 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqle

import (
	"fmt"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

// schemaFragmentType is the fragment type for an entry in the dolt_schemas table.
type schemaFragmentType string

const (
	schemaFragmentType_View      schemaFragmentType = "view"
	schemaFragmentType_Trigger   schemaFragmentType = "trigger"
	schemaFragmentType_Procedure schemaFragmentType = "procedure"
)

// schemaFragment represents an entry in the dolt_schemas table.
type schemaFragment struct {
	Name       string
	Type       schemaFragmentType
	Fragment   string
	CreatedAt  time.Time
	ModifiedAt time.Time
	Metadata   string
}

// The fixed SQL schema for the `dolt_schemas` table.
func SchemasTableSqlSchema() sql.Schema {
	sqlSchema, err := sqlutil.FromDoltSchema(doltdb.SchemasTableName, SchemasTableSchema())
	if err != nil {
		panic(err) // should never happen
	}
	return sqlSchema
}

// The fixed dolt schema for the `dolt_schemas` table.
func SchemasTableSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn(doltdb.SchemasTablesTypeCol, schema.DoltSchemasTypeTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.SchemasTablesNameCol, schema.DoltSchemasNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.SchemasTablesFragmentCol, schema.DoltSchemasFragmentTag, types.StringKind, false),
		schema.NewColumn(doltdb.SchemasTablesCreatedAtCol, schema.DoltSchemasCreatedAtTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.SchemasTablesModifiedAtCol, schema.DoltSchemasModifiedAtTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.SchemasTablesMetadataCol, schema.DoltSchemasMetadataTag, types.StringKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

// GetOrCreateDoltSchemasTable returns the `dolt_schemas` table in `db`, creating it if it does not already exist.
func GetOrCreateDoltSchemasTable(ctx *sql.Context, db Database) (retTbl *WritableDoltTable, retErr error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err := db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	var rowsToAdd []sql.Row
	if found {
		schemasTable := tbl.(*WritableDoltTable)
		if len(tbl.Schema()) == 3 { // v1 schemas table contains 3 columns
			root, rowsToAdd, err = migrateV1SchemasTableToV3(ctx, db, root, schemasTable)
			if err != nil {
				return nil, err
			}
		} else if len(tbl.Schema()) == 4 { // v2 schemas table contains 4 columns
			root, rowsToAdd, err = migrateV2SchemasTableToV3(ctx, db, root, schemasTable)
			if err != nil {
				return nil, err
			}
		} else {
			return schemasTable, nil
		}
	}
	// Create the schemas table as an empty table
	err = db.createDoltTable(ctx, doltdb.SchemasTableName, root, SchemasTableSchema())
	if err != nil {
		return nil, err
	}
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sql.ErrTableNotFound.New("dolt_schemas")
	}
	// If there was an old schemas table that contained rows, then add that data here
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sql.ErrTableNotFound.New("dolt_schemas")
	}
	if len(rowsToAdd) != 0 {
		err = func() (retErr error) {
			inserter := tbl.(*WritableDoltTable).Inserter(ctx)
			defer func() {
				err := inserter.Close(ctx)
				if retErr == nil {
					retErr = err
				}
			}()
			for _, sqlRow := range rowsToAdd {
				err = inserter.Insert(ctx, sqlRow)
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	return tbl.(*WritableDoltTable), nil
}

func migrateV1SchemasTableToV3(
	ctx *sql.Context,
	db Database,
	root *doltdb.RootValue,
	schemasTable *WritableDoltTable,
) (
	*doltdb.RootValue,
	[]sql.Row,
	error,
) {
	// Copy all of the old data over and add the new columns
	var rowsToAdd []sql.Row
	rowData, err := schemasTable.table.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	err = rowData.IterAll(ctx, func(key, val types.Value) error {
		dRow, err := row.FromNoms(schemasTable.sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}
		sqlRow, err := sqlutil.DoltRowToSqlRow(dRow, schemasTable.sch)
		if err != nil {
			return err
		}
		// append the new createdat, modifiedat, and metadata to each row
		sqlRow = append(sqlRow, time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(), "")
		rowsToAdd = append(rowsToAdd, sqlRow)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	err = db.DropTable(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, nil, err
	}
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, nil, err
	}
	return root, rowsToAdd, nil
}

func migrateV2SchemasTableToV3(
	ctx *sql.Context,
	db Database,
	root *doltdb.RootValue,
	schemasTable *WritableDoltTable,
) (
	*doltdb.RootValue,
	[]sql.Row,
	error,
) {
	// Copy all of the old data over and add the new columns
	var rowsToAdd []sql.Row
	rowData, err := schemasTable.table.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	id := int64(1)
	err = rowData.IterAll(ctx, func(key, val types.Value) error {
		dRow, err := row.FromNoms(schemasTable.sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}
		sqlRow, err := sqlutil.DoltRowToSqlRow(dRow, schemasTable.sch)
		if err != nil {
			return err
		}
		// remove id and append the new createdat, modifiedat, and metadata to each row
		sqlRow = append(sqlRow[:3], time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(), "")
		rowsToAdd = append(rowsToAdd, sqlRow)
		id++
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	err = db.DropTable(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, nil, err
	}
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, nil, err
	}
	return root, rowsToAdd, nil
}

// fragFromSchemasTable returns the row with the given schema fragment if it exists.
func fragFromSchemasTable(ctx *sql.Context, tbl *WritableDoltTable, fragType schemaFragmentType, name string) (sql.Row, bool, error) {
	indexes, err := tbl.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	var fragNameIndex sql.Index
	for _, index := range indexes {
		if index.ID() == "PRIMARY" {
			fragNameIndex = index
			break
		}
	}
	if fragNameIndex == nil {
		return nil, false, fmt.Errorf("could not find primary key index on system table `%s`", doltdb.SchemasTableName)
	}

	indexLookup, err := fragNameIndex.Get(string(fragType), name)
	if err != nil {
		return nil, false, err
	}
	dil := indexLookup.(*doltIndexLookup)
	rowIter, err := dil.RowIter(ctx, dil.IndexRowData(), nil)
	if err != nil {
		return nil, false, err
	}
	defer rowIter.Close(ctx)
	sqlRow, err := rowIter.Next()
	if err == nil {
		return sqlRow, true, nil
	} else if err == io.EOF {
		return nil, false, nil
	} else {
		return nil, false, err
	}
}
