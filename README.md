# RQL

[![Go Test and Coverage](https://github.com/dvaldivia/rql/actions/workflows/go.yml/badge.svg)](https://github.com/dvaldivia/rql/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/dvaldivia/rql/branch/master/graph/badge.svg)](https://codecov.io/gh/dvaldivia/rql)

REST Query Language

A library to introduce filters that can be passed by the client to the server via a query parameter using SQL-like WHERE statements. RQL provides two main functionalities:

1. Parse the SQL-like filter statement
2. Apply the filter to arrays of generic structs using the same SQL-like filters

## Why?

Often applications building REST APIs need to expose a generic way to filter on top of existing resources, allowing the flexibility to filter by any field in the table. However, coming up with a new syntax for such use cases is complicated. `RQL` addresses these concerns by:

1. Providing a way to safely parse a SQL-like filter statement
2. Offering the ability to filter in-memory arrays of structs using the parsed filter

This unified approach allows you to use the same filter language both for database queries and in-memory filtering operations.

# Examples

## SQL-like Filter Syntax

A filter string can be passed via `GET` or `POST` such as:

```sql
field1='value1' AND (field2='value2' AND field3='value3')
```

This filter can be applied to an array of structs using `ApplyFilter`:

## Generic Array Filtering

Using the same filter syntax, you can filter arrays of structs in memory:

```go
// Define your struct type
type Person struct {
    ID        int
    Name      string
    Email     string
    Age       int
    Active    bool
    Department *Department
}

type Department struct {
    Name     string
    Location string
}

// Your data array
people := []Person{
    {ID: 1, Name: "Alice", Age: 25, Active: true, Department: &Department{Name: "Engineering"}},
    {ID: 2, Name: "Bob", Age: 30, Active: false, Department: &Department{Name: "Marketing"}},
    // More entries...
}

// Apply a filter with SQL-like syntax
result, err := rql.ApplyFilter("Age >= 25 AND Department.Name='Engineering'", people)
// result.Items will contain only the matching Person objects
// result.Count will contain the total number of matches
```

## Pagination Support

RQL supports pagination through the `FilterOptions` struct:

```go
// With pagination (limit and offset)
options := rql.FilterOptions{
    Limit: 10,  // Maximum number of results to return (0 = no limit)
    Offset: 20, // Number of results to skip (0 = start from beginning)
}

// Apply filter with pagination options
result, err := rql.ApplyFilter("Age >= 25", people, options)

// result.Items - The filtered items after applying pagination
// result.Count - Total count of items that matched the filter before pagination

fmt.Printf("Showing %d items out of %d total matches\n", len(result.Items), result.Count)
```

## Result Structure

The `ApplyFilter` function returns a `Result` struct that contains both the filtered items and the total count:

```go
// Result contains the filtered items and count information
type Result[T any] struct {
    Items []T // The filtered items after applying pagination
    Count int // Total count of items that matched the filter before pagination
}
```

This structure is especially useful for building paginated APIs where you need to display the total number of matches alongside the current page of results.
# Features

- SQL-like filter syntax for both database and in-memory filtering
- Support for common comparison operators: `=`, `!=`, `>`, `<`, `>=`, `<=`
- String pattern matching with `LIKE` and `ILIKE` (case-insensitive)
- Logical operators `AND` and `OR` with proper parentheses support
- Nested field access with dot notation (e.g., `Department.Name='Engineering'`)
- Case-insensitive field matching for struct fields
- Typed numeric comparisons for numeric fields
- Pagination support with limit and offset options
- Result structure with both filtered items and total count information

# Future Development

- [ ] Pass list of valid fields to parser
- [x] Support dot-walking to access nested fields 
- [ ] Support for arrays and slices in struct fields
- [x] Filter pagination support
- [ ] Advanced sorting options