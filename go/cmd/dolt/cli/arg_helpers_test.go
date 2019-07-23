package cli

import (
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestParseKeyValues(t *testing.T) {
	ctx := context.Background()

	const (
		lnColName = "last"
		fnColName = "first"
		mnColName = "middle"
		lnColTag  = 1
		fnColTag  = 0
		mnColTag  = 2
	)

	testKeyColColl, _ := schema.NewColCollection(
		schema.NewColumn(lnColName, lnColTag, types.StringKind, true),
		schema.NewColumn(fnColName, fnColTag, types.StringKind, true),
		schema.NewColumn(mnColName, mnColTag, types.StringKind, true),
	)

	sch := schema.SchemaFromCols(testKeyColColl)

	singleKeyColColl, _ := schema.NewColCollection(
		schema.NewColumn(lnColName, lnColTag, types.StringKind, true),
	)

	singleKeySch := schema.SchemaFromCols(singleKeyColColl)

	tests := []struct {
		sch          schema.Schema
		args         []string
		expectedKeys []types.Value
		expectErr    bool
	}{
		{
			singleKeySch,
			[]string{"robertson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(types.Format_7_18, singleKeyColColl.Tags, true).Value(ctx),
			},
			false,
		},

		{
			singleKeySch,
			[]string{"last", "robertson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(types.Format_7_18, singleKeyColColl.Tags, true).Value(ctx),
			},
			false,
		},
		{
			singleKeySch,
			[]string{"last"},
			[]types.Value{},
			false,
		},
		{
			singleKeySch,
			[]string{"last", "last"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("last")}.NomsTupleForTags(types.Format_7_18, singleKeyColColl.Tags, true).Value(ctx),
			},
			false,
		},
		{
			singleKeySch,
			[]string{"last", "robertson", "johnson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(types.Format_7_18, singleKeyColColl.Tags, true).Value(ctx),
				row.TaggedValues{lnColTag: types.String("johnson")}.NomsTupleForTags(types.Format_7_18, singleKeyColColl.Tags, true).Value(ctx),
			},
			false,
		},

		{
			sch,
			[]string{"last"},
			nil,
			false,
		},
		{
			sch,
			[]string{"last", "robertson", "johnson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(types.Format_7_18, testKeyColColl.Tags, true).Value(ctx),
				row.TaggedValues{lnColTag: types.String("johnson")}.NomsTupleForTags(types.Format_7_18, testKeyColColl.Tags, true).Value(ctx),
			},
			false,
		},
		{
			sch,
			[]string{"first,last", "robert,robertson", "john,johnson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson"), fnColTag: types.String("robert")}.NomsTupleForTags(types.Format_7_18, testKeyColColl.Tags, true).Value(ctx),
				row.TaggedValues{lnColTag: types.String("johnson"), fnColTag: types.String("john")}.NomsTupleForTags(types.Format_7_18, testKeyColColl.Tags, true).Value(ctx),
			},
			false,
		},
	}

	for _, test := range tests {
		actual, err := ParseKeyValues(types.Format_7_18, test.sch, test.args)

		if test.expectErr != (err != nil) {
			t.Error(test.args, "produced an unexpected error")
		} else {
			longer := len(actual)
			if len(test.expectedKeys) > longer {
				longer = len(test.expectedKeys)
			}

			for i := 0; i < longer; i++ {
				var currActual types.Value = types.NullValue
				var currExpected types.Value = types.NullValue

				if i < len(test.expectedKeys) {
					currExpected = test.expectedKeys[i]
				}

				if i < len(actual) {
					currActual = actual[i]
				}

				if !currActual.Equals(currExpected) {
					t.Error("actual:", types.EncodedValue(context.Background(), currActual), "!= expected:", types.EncodedValue(context.Background(), currExpected))
				}
			}
		}
	}
}
