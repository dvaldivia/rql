// Copyright 2023 Daniel Valdivia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rql

import (
	"fmt"
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/auxten/postgresql-parser/pkg/walk"
	"log"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// FilterOptions contains configuration options for filtering and pagination
type FilterOptions struct {
	Limit  int // Maximum number of results to return, 0 means no limit
	Offset int // Number of results to skip before returning, 0 means start from beginning
}

// Result contains the filtered items and count information
type Result[T any] struct {
	Items []T // The filtered items after applying pagination
	Count int // Total count of items that matched the filter before pagination
}

// DefaultFilterOptions returns the default filter options (no limit, no offset)
func DefaultFilterOptions() FilterOptions {
	return FilterOptions{
		Limit:  0, // No limit
		Offset: 0, // No offset
	}
}

// ApplyFilter filters a slice of generic structs based on SQL-like filter conditions
// with optional pagination using limit and offset
func ApplyFilter[T any](rawFilters string, items []T, options ...FilterOptions) (Result[T], error) {
	// Set default options if none provided
	opts := DefaultFilterOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	// Empty filter returns all items (possibly with pagination)
	if rawFilters == "" {
		return Result[T]{
			Items: applyPagination(items, opts),
			Count: len(items),
		}, nil
	}

	rawFilters = url.QueryEscape(rawFilters)
	var err error
	rawFilters, err = url.QueryUnescape(rawFilters)
	if err != nil {
		log.Println(err)
		return Result[T]{}, err
	}

	pseudoQuery := fmt.Sprintf("SELECT * FROM t WHERE %s", rawFilters)
	queryTree, err := parser.Parse(pseudoQuery)
	if err != nil {
		log.Println(err)
		return Result[T]{}, err
	}

	// Create a generic filter visitor
	gfv := &GenericFilterVisitor{}

	whereFinder := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			if whereNode, ok := node.(*tree.Where); ok {
				// Process the where clause
				gfv.processWhereNode(whereNode)
				return true
			}
			return false
		},
	}

	_, _ = whereFinder.Walk(queryTree, nil)

	// Apply filter to items
	filteredItems := make([]T, 0)

	for _, item := range items {
		if gfv.evaluate(item) {
			filteredItems = append(filteredItems, item)
		}
	}

	// Save the total count of matched items before pagination
	totalCount := len(filteredItems)

	// Apply pagination to filtered results
	return Result[T]{
		Items: applyPagination(filteredItems, opts),
		Count: totalCount,
	}, nil
}

// applyPagination applies limit and offset pagination to a slice of items
func applyPagination[T any](items []T, options FilterOptions) []T {
	totalItems := len(items)
	offset := options.Offset

	// Handle offset
	if offset > totalItems {
		return []T{}
	}
	items = items[offset:]

	// Handle limit
	limit := options.Limit
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}

	return items
}

// GenericFilterVisitor handles filtering of generic structs
type GenericFilterVisitor struct {
	rootCondition Condition
}

// processWhereNode processes a where clause
func (v *GenericFilterVisitor) processWhereNode(whereNode *tree.Where) {
	v.rootCondition = v.processExpr(whereNode.Expr)
}

// evaluate evaluates if an item meets the filter conditions
func (v *GenericFilterVisitor) evaluate(item interface{}) bool {
	if v.rootCondition == nil {
		return true
	}
	return v.rootCondition.evaluate(item)
}

// processExpr processes a tree.Expr node and returns a Condition
func (v *GenericFilterVisitor) processExpr(expr tree.Expr) Condition {
	switch node := expr.(type) {
	case *tree.ComparisonExpr:
		return v.processComparisonExpr(node)
	case *tree.AndExpr:
		return &AndCondition{
			left:  v.processExpr(node.Left),
			right: v.processExpr(node.Right),
		}
	case *tree.OrExpr:
		return &OrCondition{
			left:  v.processExpr(node.Left),
			right: v.processExpr(node.Right),
		}
	}

	// Default to always true for unhandled expressions
	return &alwaysTrueCondition{}
}

// processComparisonExpr processes a comparison expression
func (v *GenericFilterVisitor) processComparisonExpr(node *tree.ComparisonExpr) Condition {
	colName := node.Left.String()
	var value string
	if valStr, ok := node.Right.(*tree.StrVal); ok {
		value = valStr.RawString()
	} else {
		// For other types, use the string representation
		value = node.Right.String()
	}

	switch node.Operator {
	case tree.EQ:
		return &EqualCondition{field: colName, value: value}
	case tree.NE:
		return &NotEqualCondition{field: colName, value: value}
	case tree.Like:
		return &LikeCondition{field: colName, value: value, caseInsensitive: false}
	case tree.ILike:
		return &LikeCondition{field: colName, value: value, caseInsensitive: true}
	case tree.GE: // Greater than or equal
		return &ComparisonCondition{field: colName, value: value, operator: ">="}
	case tree.GT: // Greater than
		return &ComparisonCondition{field: colName, value: value, operator: ">"}
	case tree.LT: // Less than
		return &ComparisonCondition{field: colName, value: value, operator: "<"}
	case tree.LE: // Less than or equal
		return &ComparisonCondition{field: colName, value: value, operator: "<="}
	// Add more operators as needed
	default:
		// Default to equality
		return &EqualCondition{field: colName, value: value}
	}
}

// Condition interface for filter conditions
type Condition interface {
	evaluate(item interface{}) bool
}

// AndCondition represents an AND condition
type AndCondition struct {
	left  Condition
	right Condition
}

func (c *AndCondition) evaluate(item interface{}) bool {
	return c.left.evaluate(item) && c.right.evaluate(item)
}

// OrCondition represents an OR condition
type OrCondition struct {
	left  Condition
	right Condition
}

func (c *OrCondition) evaluate(item interface{}) bool {
	return c.left.evaluate(item) || c.right.evaluate(item)
}

// EqualCondition checks if a field equals a value
type EqualCondition struct {
	field string
	value string
}

func (c *EqualCondition) evaluate(item interface{}) bool {
	fieldValue, found := getFieldValue(item, c.field)
	if !found {
		return false
	}
	return fieldValue == c.value
}

// NotEqualCondition checks if a field is not equal to a value
type NotEqualCondition struct {
	field string
	value string
}

func (c *NotEqualCondition) evaluate(item interface{}) bool {
	fieldValue, found := getFieldValue(item, c.field)
	if !found {
		return false
	}
	return fieldValue != c.value
}

// LikeCondition checks if a field matches a pattern
type LikeCondition struct {
	field           string
	value           string
	caseInsensitive bool
}

// ComparisonCondition handles numeric comparisons (>, <, >=, <=)
type ComparisonCondition struct {
	field    string
	value    string
	operator string // ">", "<", ">=", "<="
}

func (c *ComparisonCondition) evaluate(item interface{}) bool {
	fieldValue, found := getFieldValue(item, c.field)
	if !found {
		return false
	}

	// Try to convert both values to numbers for numeric comparison
	fieldNum, fieldErr := strconv.ParseFloat(fieldValue, 64)
	valueNum, valueErr := strconv.ParseFloat(c.value, 64)

	// If either value is not a number, fall back to string comparison
	if fieldErr != nil || valueErr != nil {
		// String comparison (lexicographical)
		switch c.operator {
		case ">":
			return fieldValue > c.value
		case ">=":
			return fieldValue >= c.value
		case "<":
			return fieldValue < c.value
		case "<=":
			return fieldValue <= c.value
		default:
			return false
		}
	}

	// Numeric comparison
	switch c.operator {
	case ">":
		return fieldNum > valueNum
	case ">=":
		return fieldNum >= valueNum
	case "<":
		return fieldNum < valueNum
	case "<=":
		return fieldNum <= valueNum
	default:
		return false
	}
}

func (c *LikeCondition) evaluate(item interface{}) bool {
	fieldValue, found := getFieldValue(item, c.field)
	if !found {
		return false
	}

	// Handle case sensitivity
	str := fieldValue
	pattern := c.value
	if c.caseInsensitive {
		str = strings.ToLower(str)
		pattern = strings.ToLower(pattern)
	}

	// Simple approach for %X% patterns - exact substring match
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		search := strings.Trim(pattern, "%")
		return strings.Contains(str, search)
	}

	// Handle SQL-style patterns directly without full regex
	if pattern == "%" {
		// Just a wildcard matches everything
		return true
	}

	// Handle exact prefix matches
	if !strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		// Pattern like "value%", check prefix
		prefix := strings.TrimSuffix(pattern, "%")
		return strings.HasPrefix(str, prefix)
	}

	// Handle exact suffix matches
	if strings.HasPrefix(pattern, "%") && !strings.HasSuffix(pattern, "%") {
		// Pattern like "%value", check suffix
		suffix := strings.TrimPrefix(pattern, "%")
		return strings.HasSuffix(str, suffix)
	}

	// Handle exact matches with no wildcards
	if !strings.Contains(pattern, "%") {
		// No wildcards, exact match
		return str == pattern
	}

	// For more complex patterns, use the proper regex approach
	// First escape regex special characters
	regexSpecial := []string{".", "^", "$", "*", "+", "?", "(", ")", "[", "]", "{", "}", "|"}
	regexPattern := pattern
	for _, char := range regexSpecial {
		regexPattern = strings.ReplaceAll(regexPattern, char, "\\"+char)
	}

	// Convert SQL wildcards to regex
	regexPattern = strings.ReplaceAll(regexPattern, "%", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "_", ".")

	// Create anchored regex
	regexPattern = "^" + regexPattern + "$"

	// Use regexp to match
	match, err := regexp.MatchString(regexPattern, str)
	if err != nil {
		// Fallback to simple contains
		return strings.Contains(str, strings.Trim(pattern, "%"))
	}

	return match
}

// alwaysTrueCondition is a condition that always evaluates to true
type alwaysTrueCondition struct{}

func (c *alwaysTrueCondition) evaluate(item interface{}) bool {
	return true
}

// getFieldValue gets a field value from an item using reflection
// Supports nested fields with dot notation (e.g., "user.name")
func getFieldValue(item interface{}, fieldPath string) (string, bool) {
	value := reflect.ValueOf(item)

	// If item is a pointer, dereference it
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	// If not struct, we can't get field values
	if value.Kind() != reflect.Struct {
		return "", false
	}

	// Handle nested fields with dot notation
	fieldParts := strings.Split(fieldPath, ".")

	for i, fieldName := range fieldParts {
		// Get field by name (case-sensitive)
		field := value.FieldByName(fieldName)

		// If field not found, try case-insensitive match
		if !field.IsValid() {
			field = findFieldCaseInsensitive(value, fieldName)
			if !field.IsValid() {
				return "", false
			}
		}

		// If this is the last part, convert to string and return
		if i == len(fieldParts)-1 {
			return fmt.Sprintf("%v", field.Interface()), true
		}

		// Otherwise, continue traversing
		if field.Kind() == reflect.Ptr {
			field = field.Elem()
		}

		if field.Kind() != reflect.Struct {
			return "", false
		}

		value = field
	}

	return "", false // Should not reach here
}

// findFieldCaseInsensitive finds a field by name (case-insensitive)
func findFieldCaseInsensitive(value reflect.Value, fieldName string) reflect.Value {
	fieldNameLower := strings.ToLower(fieldName)

	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if strings.ToLower(field.Name) == fieldNameLower {
			return value.Field(i)
		}
	}

	return reflect.Value{} // Not found
}
