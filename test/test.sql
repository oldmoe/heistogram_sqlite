-- Load the extension
.load ../src/heistogram
.mode table

DROP TABLE IF EXISTS temp.results;

-- Create results table
CREATE TABLE temp.results (
    test_name TEXT,
    status TEXT,
    actual TEXT,
    expected TEXT
);

-- Test 1: Create empty heistogram
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'create_empty' AS name, heist_count(heist_create()) AS result, 0 AS expected        
        );
        SELECT heist_count(heist_create()) = 0 AS result
COMMIT;

-- Test 2: Create heistogram with values
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'create_with_values' AS name, heist_count(heist_create(10, 20, 30)) AS result, 3 AS expected
        );
COMMIT;

-- Test 3: Create heistogram with NULL values
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'create_with_nulls' AS name, heist_count(heist_create(10, NULL, 30)) AS result, 2 AS expected
        );
COMMIT;

-- Test 4: Add value to heistogram
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'add_value' AS name, heist_count(heist_add(heist_create(10, 20), 30)) AS result, 3 AS expected
        );
COMMIT;

-- Test 5: Add NULL value to heistogram
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'add_null' AS name, heist_count(heist_add(heist_create(10, 20), NULL)) AS result, 2 AS expected
        );
COMMIT;

-- Test 6: Remove value from heistogram
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'remove_value' AS name, heist_count(heist_remove(heist_create(10, 20, 30), 20)) AS result, 2 AS expected
        );
COMMIT;

-- Test 7: Remove NULL value from heistogram
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'remove_null' AS name, heist_count(heist_remove(heist_create(10, 20, 30), NULL)) AS result, 3 AS expected
        );
COMMIT;

-- Test 8: Merge heistograms
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'merge_histograms' AS name, heist_count(heist_merge(heist_create(10, 20), heist_create(10, 40))) AS result, 4 AS expected
        );
COMMIT;

-- Test 9: Percentile on heistogram
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'percentile' AS name, heist_percentile(heist_create(10, 20, 30, 40, 50), 50) AS result, 30 AS expected
        );
COMMIT;

-- Test 10: Group create with all values
BEGIN;
    INSERT INTO results
        SELECT name, iif(result = expected,'PASS', 'FAIL'), result, expected FROM (
            SELECT 'group_create' AS name, 
                    heist_count((SELECT heist_group_create(value) FROM generate_series(1, 1000))) AS result, 
                    1000 AS expected
        );
COMMIT;

-- Display all test results
SELECT * FROM results ORDER BY test_name;

DROP TABLE temp.results;