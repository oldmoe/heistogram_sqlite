# heistogram_sqlite
The Heistogram quantile approximation library as a SQLite extension

## how to build it

clone the repp (using git clone --recursive to get the heistogram library itself)
```bash
  git clone --recursive https://github.com/oldmoe/heistogram_sqlite.git
```

go to the /src directory and run (on linux)
```bash
  gcc -O3 -fpic -shared -o heistogram.so heistogram_sqlite.c -lm
```
You can now load the extension in sqlite and use it

## Scalar functions

### heist_create()
Create an new heistogram, accepts zero or an arbitrary number of arguments, the non null values will be added to heistogram (any non int values will be coverted to int first)

### heist_add(value)
Adds a value to the heistogram

### heist_remove(value)
Removes a value from the heistogram, please note that removing values that are at the boundary of the heistogram may result in percentile queries returning results that are before or beyond the actual min or max of the heistogram, respectively. The maximum error will still be applicable though

### heist_percentile(heistgram, percentile)
Returns the apporixmate value at the requested percentile, precentiles range from 0 to 100, inclusive.

### heist_merge(h1, h2)
Merges heistgrams h1 and h2 and returns a new one

### heist_count(heistogram)
The total count of numbers inserted into the heistogram - the total count of numbers removed from the heistogram

### heist_max(heistogram)
The maximum value inseted into the heistogram

### heist_min(heistogram)
The minimum value inseted into the heistogram

## Aggregate functions

### heist_group_add(column)
Adds a group of values belonging to column to a heistogram

Example:
```sql
SELECT heist_percentile(heist_group_add(sample), 95) FROM (SELECT sample FROM samples WHERE active = 1);
```
### heist_group_remove(column)
Same like heist_group_add but remove the values from the heistogram

### heist_group_merge(column)
Meerges all the heistograms in the column while ignoring null values

### heist_group_percentile(column)
This is a convenience method that allows you to directly aggregat a column and calculate a percentile. It is discouraged though as it is slower than actually creating the heistogram first and then passing it to the scalar `heist_precentile()` function. Except if you are dealing with very few rows.

## Running the tests
There's a test.sql file which you can execute as follows:

```bash
sqlite3 ":memory:" ".read test.sql"
```
You should see something like this:
```
+--------------------+--------+--------+----------+
|     test_name      | status | actual | expected |
+--------------------+--------+--------+----------+
| add_null           | PASS   | 2      | 2        |
| add_value          | PASS   | 3      | 3        |
| create_empty       | PASS   | 0      | 0        |
| create_with_nulls  | PASS   | 2      | 2        |
| create_with_values | PASS   | 3      | 3        |
| group_create       | PASS   | 1000   | 1000     |
| merge_histograms   | PASS   | 4      | 4        |
| percentile         | PASS   | 30.0   | 30       |
| remove_null        | PASS   | 3      | 3        |
| remove_value       | PASS   | 2      | 2        |
+--------------------+--------+--------+----------+
```
