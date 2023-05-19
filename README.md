# RQL
REST Query Language

A library to introduce filters that can be passed by the client to the server via a query parameter using SQL like WHERE
statements and getting a [goqu](http://doug-martin.github.io/goqu/) `[]goqu.Expression` in return.

## Why?

Often applications building REST APIs need to expire a generic way to filter on top of existing resources, allowing the flexibility to filter by any field in the table, however coming up with a whole new syntax for such use case is complicated, on top of that you need to build the proper WHERE statement for a SQL query that is safe, `RQL Filter` addresses this.

`RQL` provides a way to safely and easily parse a filter statement and build a  `[]goqu.Expression` expression.

# Examples

A query passed via `GET` or `POST` such as 

```sql
field1='value1' AND (field2='value2' AND field3='value3')
```

woul yield the following expressions
```go
[]goqu.Expression{
    goqu.C("field1").Eq("value1"),
    goqu.And([]goqu.Expression{
        goqu.C("field2").Eq("value2"),
        goqu.C("field3").Eq("value3"),
    }...),
}
```
# TODOs

- [ ] Pass list of valid fields to parser
- [ ] Support dot-walking to related tables