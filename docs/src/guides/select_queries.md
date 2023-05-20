## Select Queries

### Aggregation Functions
- SUM
- AVG
- COUNT
- MAX
- MIN

### Functions
- NOT YET

### Group By
YES
```sql
select sum(amount), userId from orders group by userId order by userId
```
### Order By
YES, see above.

### Limit
```
select * from orders order by userId limit 1 offset 1
```

### Others
