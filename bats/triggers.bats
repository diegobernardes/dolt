#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE parent (
    id int PRIMARY KEY,
    v1 int,
    v2 int,
    INDEX v1 (v1),
    INDEX v2 (v2)
);
CREATE TABLE child (
    id int primary key,
    v1 int,
    v2 int
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "triggers: Mix of CREATE TRIGGER and CREATE VIEW in batch mode" {
    # There were issues with batch mode that this is verifying no longer occurs
    dolt sql <<SQL
CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);
CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;
CREATE VIEW view1 AS SELECT v1 FROM test;
CREATE TABLE a (x INT PRIMARY KEY);
CREATE TABLE b (y INT PRIMARY KEY);
INSERT INTO test VALUES (1, 1);
CREATE VIEW view2 AS SELECT y FROM b;
CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2);
INSERT INTO a VALUES (2);
SQL
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM a" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "x" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM b" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "y" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "y" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_schemas ORDER BY name" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment,createdat,modifiedat,metadata" ]] || false
    [[ "$output" =~ 'trigger,trigger1,CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "$output" =~ 'view,view1,SELECT v1 FROM test,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "$output" =~ 'view,view2,SELECT y FROM b,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "$output" =~ 'trigger,trigger2,CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2),1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}

@test "triggers: Writing directly into dolt_schemas" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);"
    dolt sql -q "CREATE VIEW view1 AS SELECT v1 FROM test;"
    dolt sql -q "INSERT INTO dolt_schemas VALUES ('trigger', 'trigger1', 'CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;', '1970-01-01 00:00:00', '1970-01-01 00:00:00', '');"
    dolt sql -q "INSERT INTO test VALUES (1, 1);"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "triggers: Upgrade dolt_schemas_v1" {
    rm -rf .dolt
    # dolt_schemas_v1 was created using v0.19.1, which is pre-id change
    cp -a $BATS_TEST_DIRNAME/helper/dolt_schemas_v1/. ./.dolt/
    run dolt sql -q "SELECT * FROM dolt_schemas" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment" ]] || false
    [[ "$output" =~ "view,view1,SELECT 2+2 FROM dual" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "2 + 2" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # creating a new view/trigger will recreate the dolt_schemas table
    dolt sql -q "CREATE VIEW view2 AS SELECT 3+3 FROM dual;"
    run dolt diff
    [ "$status" -eq "0" ]
    [[ "$output" =~ "deleted table" ]] || false
    [[ "$output" =~ "added table" ]] || false
    run dolt sql -q "SELECT * FROM dolt_schemas" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment,createdat,modifiedat,metadata" ]] || false
    [[ "$output" =~ 'view,view1,SELECT 2+2 FROM dual,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "$output" =~ 'view,view2,SELECT 3+3 FROM dual,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM view1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "2 + 2" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "3 + 3" ]] || false
    [[ "$output" =~ "6" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    dolt add -A
    dolt commit -m "newest dolt_schemas"
    # old commits are preserved
    run dolt sql -q "SELECT * FROM dolt_schemas AS OF 'HEAD~1'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment" ]] || false
    [[ "$output" =~ "view,view1,SELECT 2+2 FROM dual" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "triggers: Upgrade dolt_schemas_v2" {
    rm -rf .dolt
    # dolt_schemas_v2 was created using v0.23.8, which is post-id and pre-time change
    cp -a $BATS_TEST_DIRNAME/helper/dolt_schemas_v2/. ./.dolt/
    run dolt sql -q "SELECT * FROM dolt_schemas" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment,id" ]] || false
    [[ "$output" =~ "view,view1,SELECT 2+2 FROM dual,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "2 + 2" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # creating a new view/trigger will recreate the dolt_schemas table
    dolt sql -q "CREATE VIEW view2 AS SELECT 3+3 FROM dual;"
    run dolt diff
    [ "$status" -eq "0" ]
    [[ "$output" =~ "deleted table" ]] || false
    [[ "$output" =~ "added table" ]] || false
    run dolt sql -q "SELECT * FROM dolt_schemas" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment,createdat,modifiedat,metadata" ]] || false
    [[ "$output" =~ 'view,view1,SELECT 2+2 FROM dual,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "$output" =~ 'view,view2,SELECT 3+3 FROM dual,1970-01-01 00:00:00 +0000 UTC,1970-01-01 00:00:00 +0000 UTC,""' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM view1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "2 + 2" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "3 + 3" ]] || false
    [[ "$output" =~ "6" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    dolt add -A
    dolt commit -m "newest dolt_schemas"
    # old commits are preserved
    run dolt sql -q "SELECT * FROM dolt_schemas AS OF 'HEAD~1'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment,id" ]] || false
    [[ "$output" =~ "view,view1,SELECT 2+2 FROM dual,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}
