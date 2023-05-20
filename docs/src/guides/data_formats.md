## Data Formats

### csv

Notes:
- Expects the first line of the file be headers
- Only read is supported. No writes support for csv files.
- Will inference data types from the csv files with initial 10 lines of data
- Only supports a subset of data types: `String`, `Boolean`, `Int64`, `Float64`, `DateTime`.

```
select id, amount, userId from 'tests/assets/orders.csv' where id = 1
```

### JSON
NOT YET

