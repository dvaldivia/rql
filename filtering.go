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
	"bufio"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/auxten/postgresql-parser/pkg/walk"
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

	// Pre-process ANY syntax before passing to the SQL parser
	processedFilter, anyConditions, err := preprocessAnyOperator(rawFilters)
	if err != nil {
		log.Println(err)
		return Result[T]{}, err
	}

	rawFilters = url.QueryEscape(processedFilter)
	rawFilters, err = url.QueryUnescape(rawFilters)
	if err != nil {
		log.Println(err)
		return Result[T]{}, err
	}

	pseudoQuery := fmt.Sprintf("SELECT * FROM t WHERE %s", rawFilters)
	queryTree, err := parser.Parse(pseudoQuery)
	if err != nil {
		// log.Println(err)
		return Result[T]{}, err
	}

	// Create a generic filter visitor
	gfv := &GenericFilterVisitor{}

	whereFinder := &walk.AstWalker{
		Fn: func(ctx any, node any) (stop bool) {
			if whereNode, ok := node.(*tree.Where); ok {
				// Process the where clause
				gfv.processWhereNode(whereNode)
				return true
			}
			return false
		},
	}

	_, _ = whereFinder.Walk(queryTree, nil)

	// We'll handle the ANY conditions separately later

	// Apply filter to items
	filteredItems := make([]T, 0)

	// If we replaced the entire filter with placeholders, just use ANY conditions
	if (rawFilters == "1=1" || rawFilters == "true") && len(anyConditions) > 0 {
		gfv.rootCondition = nil
	}

	// Create a final filter condition combining SQL parser output and ANY conditions
	// Handle complex case: filter with both regular conditions and ANY conditions
	var finalCondition Condition

	// If the query only contains ANY operators (replaced with true placeholders)
	if rawFilters == "true" || rawFilters == "1=1" || isAlwaysTrueCondition(gfv.rootCondition) {
		// Use only the ANY conditions
		if len(anyConditions) == 1 {
			finalCondition = anyConditions[0]
		} else if len(anyConditions) > 1 {
			// Combine multiple ANY conditions with AND
			root := anyConditions[0]
			for i := 1; i < len(anyConditions); i++ {
				root = &AndCondition{left: root, right: anyConditions[i]}
			}
			finalCondition = root
		} else {
			// No conditions
			finalCondition = &alwaysTrueCondition{}
		}
	} else if len(anyConditions) > 0 {
		// Complex query with both regular SQL conditions and ANY conditions
		// log.Println("Combining SQL conditions with ANY conditions")

		// Prepare the SQL conditions
		sqlCondition := gfv.rootCondition

		// Prepare the ANY conditions
		var anyCondition Condition
		if len(anyConditions) == 1 {
			anyCondition = anyConditions[0]
		} else {
			// Combine multiple ANY conditions with AND
			anyCondition = anyConditions[0]
			for i := 1; i < len(anyConditions); i++ {
				anyCondition = &AndCondition{left: anyCondition, right: anyConditions[i]}
			}
		}

		// For a complex filter like "ANY(Skills) = 'coding' AND Active = true"
		// We need to combine the ANY condition with the regular SQL condition
		// The SQL part will have been parsed with a placeholder in place of the ANY expression
		if strings.Contains(rawFilters, " AND ") {
			finalCondition = &AndCondition{left: anyCondition, right: sqlCondition}
		} else if strings.Contains(rawFilters, " OR ") {
			finalCondition = &OrCondition{left: anyCondition, right: sqlCondition}
		} else {
			// Default to using only the ANY condition if we can't determine the structure
			finalCondition = anyCondition
		}
	} else {
		// Only regular SQL conditions, no ANY conditions
		finalCondition = gfv.rootCondition
	}

	for _, item := range items {
		if finalCondition == nil || finalCondition.evaluate(item) {
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
func (v *GenericFilterVisitor) evaluate(item any) bool {
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
	// Check if we're dealing with ANY operator (left side)
	leftStr := node.Left.String()
	isAnyField := false
	var fieldName string

	// Parse ANY(field) syntax
	if strings.HasPrefix(leftStr, "ANY(") && strings.HasSuffix(leftStr, ")") {
		isAnyField = true
		fieldName = leftStr[4 : len(leftStr)-1] // Extract field name from ANY(field)
	} else {
		fieldName = leftStr
	}

	// Process the right side
	var value string
	var values []string
	isAnyValue := false

	// Check if right side is ANY('x', 'y') format
	rightStr := node.Right.String()
	if strings.HasPrefix(rightStr, "ANY(") && strings.HasSuffix(rightStr, ")") {
		isAnyValue = true
		// Extract values from ANY('x', 'y', 'z') format
		valuesStr := rightStr[4 : len(rightStr)-1]

		// Parse the comma-separated values, handling quoted strings
		values = parseAnyValues(valuesStr)
	} else {
		// Regular single value
		if valStr, ok := node.Right.(*tree.StrVal); ok {
			value = valStr.RawString()
		} else {
			// For other types, use the string representation
			value = node.Right.String()
		}
	}

	// Special handling for true/false literals (used as replacements for ANY operators)
	if fieldName == "true" || fieldName == "false" {
		// log.Printf("Detected placeholder condition: %s %s %s", fieldName, node.Operator, rightStr)
		if fieldName == "true" && (rightStr == "true" || rightStr == "1=1") {
			return &alwaysTrueCondition{}
		}
		// For complex conditions, we need to make sure the ANY parts are properly evaluated
		// and not the placeholders
		return &alwaysTrueCondition{}
	}

	// Now create conditions based on different operator combinations
	switch node.Operator {
	case tree.EQ:
		if isAnyField {
			if isAnyValue {
				// ANY(field) = ANY('x', 'y')
				return &AnyArrayContainsAnyCondition{field: fieldName, values: values}
			} else {
				// ANY(field) = 'x'
				return &AnyArrayContainsCondition{field: fieldName, value: value}
			}
		} else {
			return &EqualCondition{field: fieldName, value: value}
		}
	case tree.NE:
		if isAnyField {
			if isAnyValue {
				// ANY(field) != ANY('x', 'y')
				return &AnyArrayNotContainsAnyCondition{field: fieldName, values: values}
			} else {
				// ANY(field) != 'x'
				return &AnyArrayNotContainsCondition{field: fieldName, value: value}
			}
		} else {
			return &NotEqualCondition{field: fieldName, value: value}
		}
	case tree.Like:
		return &LikeCondition{field: fieldName, value: value, caseInsensitive: false}
	case tree.ILike:
		return &LikeCondition{field: fieldName, value: value, caseInsensitive: true}
	case tree.GE: // Greater than or equal
		return &ComparisonCondition{field: fieldName, value: value, operator: ">="}
	case tree.GT: // Greater than
		return &ComparisonCondition{field: fieldName, value: value, operator: ">"}
	case tree.LT: // Less than
		return &ComparisonCondition{field: fieldName, value: value, operator: "<"}
	case tree.LE: // Less than or equal
		return &ComparisonCondition{field: fieldName, value: value, operator: "<="}
	// Add more operators as needed
	default:
		// Default to equality
		return &EqualCondition{field: fieldName, value: value}
	}
}

// Condition interface for filter conditions
type Condition interface {
	evaluate(item any) bool
}

// AndCondition represents an AND condition
type AndCondition struct {
	left  Condition
	right Condition
}

func (c *AndCondition) evaluate(item any) bool {
	leftResult := c.left.evaluate(item)
	rightResult := c.right.evaluate(item)
	// log.Printf("AndCondition: left=%v, right=%v, result=%v", leftResult, rightResult, leftResult && rightResult)
	return leftResult && rightResult
}

// OrCondition represents an OR condition
type OrCondition struct {
	left  Condition
	right Condition
}

func (c *OrCondition) evaluate(item any) bool {
	return c.left.evaluate(item) || c.right.evaluate(item)
}

// EqualCondition checks if a field equals a value
type EqualCondition struct {
	field string
	value string
}

func (c *EqualCondition) evaluate(item any) bool {
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

func (c *NotEqualCondition) evaluate(item any) bool {
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

func (c *ComparisonCondition) evaluate(item any) bool {
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

func (c *LikeCondition) evaluate(item any) bool {
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

func (c *alwaysTrueCondition) evaluate(item any) bool {
	return true
}

// isAlwaysTrueCondition safely checks if a condition is an alwaysTrueCondition
func isAlwaysTrueCondition(cond Condition) bool {
	// Type assertion with comma-ok idiom to safely check the type
	_, ok := cond.(*alwaysTrueCondition)
	return ok
}

// getFieldValue gets a field value from an item using reflection
// Supports nested fields with dot notation (e.g., "user.name")
func getFieldValue(item any, fieldPath string) (string, bool) {
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

	for i := range value.NumField() {
		field := value.Type().Field(i)
		if strings.ToLower(field.Name) == fieldNameLower {
			return value.Field(i)
		}
	}

	return reflect.Value{} // Not found
}

// AnyArrayContainsCondition checks if any element in an array field equals a value
type AnyArrayContainsCondition struct {
	field string
	value string
}

// getArrayFieldValues gets an array/slice field's values as strings
func getArrayFieldValues(item any, fieldPath string) ([]string, bool) {
	value := reflect.ValueOf(item)

	// If item is a pointer, dereference it
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	// If not struct, we can't get field values
	if value.Kind() != reflect.Struct {
		return nil, false
	}

	// Handle nested fields with dot notation
	fieldParts := strings.Split(fieldPath, ".")
	for _, fieldName := range fieldParts[:len(fieldParts)-1] {
		// Get field by name (case-sensitive)
		field := value.FieldByName(fieldName)

		// If field not found, try case-insensitive match
		if !field.IsValid() {
			field = findFieldCaseInsensitive(value, fieldName)
			if !field.IsValid() {
				return nil, false
			}
		}

		// Dereference pointer if needed
		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				return nil, false
			}
			field = field.Elem()
		}

		if field.Kind() != reflect.Struct {
			return nil, false
		}

		value = field
	}

	// Get the array/slice field
	lastField := fieldParts[len(fieldParts)-1]
	arrayField := value.FieldByName(lastField)

	// If field not found, try case-insensitive match
	if !arrayField.IsValid() {
		arrayField = findFieldCaseInsensitive(value, lastField)
		if !arrayField.IsValid() {
			return nil, false
		}
	}

	// Dereference pointer if needed
	if arrayField.Kind() == reflect.Ptr {
		if arrayField.IsNil() {
			return nil, false
		}
		arrayField = arrayField.Elem()
	}

	// Check if it's an array or slice
	if arrayField.Kind() != reflect.Array && arrayField.Kind() != reflect.Slice {
		return nil, false
	}

	// Convert all elements to strings
	result := make([]string, arrayField.Len())
	for i := range arrayField.Len() {
		elem := arrayField.Index(i)
		// Handle different element types
		switch elem.Kind() {
		case reflect.String:
			result[i] = elem.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			result[i] = strconv.FormatInt(elem.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			result[i] = strconv.FormatUint(elem.Uint(), 10)
		case reflect.Float32, reflect.Float64:
			result[i] = strconv.FormatFloat(elem.Float(), 'f', -1, 64)
		case reflect.Bool:
			result[i] = strconv.FormatBool(elem.Bool())
		default:
			// For complex types, use %v format
			result[i] = fmt.Sprintf("%v", elem.Interface())
		}
	}

	return result, true
}

// AnyArrayNotContainsCondition checks if no element in an array field equals a value
type AnyArrayNotContainsCondition struct {
	field string
	value string
}

func (c *AnyArrayNotContainsCondition) evaluate(item any) bool {
	// We need to get an array/slice field and check if no element equals the value
	values, found := getArrayFieldValues(item, c.field)
	if !found {
		return false
	}
	if len(values) == 0 {
		return true // Empty array doesn't contain anything
	}

	return !slices.Contains(values, c.value) // No matches found
}

// AnyArrayContainsAnyCondition checks if any element in an array field equals any of the provided values
type AnyArrayContainsAnyCondition struct {
	field  string
	values []string
}

func (c *AnyArrayContainsAnyCondition) evaluate(item any) bool {
	// We need to get an array/slice field and check if any element equals any of the values
	fieldValues, found := getArrayFieldValues(item, c.field)
	if !found {
		return false
	}
	if len(fieldValues) == 0 {
		return false
	}

	for _, fieldVal := range fieldValues {
		if slices.Contains(c.values, fieldVal) {
			return true
		}
	}
	return false
}

// AnyArrayNotContainsAnyCondition checks if no element in an array field equals any of the provided values
type AnyArrayNotContainsAnyCondition struct {
	field  string
	values []string
}

func (c *AnyArrayNotContainsAnyCondition) evaluate(item any) bool {
	// We need to get an array/slice field and check if no element equals any of the values
	fieldValues, found := getArrayFieldValues(item, c.field)
	if !found {
		return false
	}
	if len(fieldValues) == 0 {
		return true // Empty array doesn't contain anything
	}

	for _, fieldVal := range fieldValues {
		if slices.Contains(c.values, fieldVal) {
			return false // Found a match, so the NOT condition fails
		}
	}
	return true // No matches found
}

// preprocessAnyOperator processes filter strings containing ANY operators before sending to SQL parser
// It returns a modified filter string with placeholders for ANY operations, and a list of ANY conditions
func preprocessAnyOperator(filter string) (string, []Condition, error) {
	// Regular expressions to identify ANY patterns
	// Note: In Go, we need to use double backslashes in regex for literal backslash
	// Original patterns had issues with capture groups - fixed to work with our test cases
	anyFieldRegex := regexp.MustCompile(`ANY\(([\w\.]+)\)\s*(=|!=)\s*('[^']+'|\w+)`)
	anyFieldAnyValueRegex := regexp.MustCompile(`ANY\(([\w\.]+)\)\s*(=|!=)\s*ANY\(([^)]*)\)`)

	var anyConditions []Condition

	// Replace ANY(field) = ANY(...) pattern first, as it's more specific
	anyFieldAnyValueMatches := anyFieldAnyValueRegex.FindAllStringSubmatch(filter, -1)
	for _, match := range anyFieldAnyValueMatches {
		if len(match) >= 4 { // We need at least 4 elements
			fullMatch := match[0] // Complete match
			fieldName := match[1] // The field name
			operator := match[2]  // = or !=
			valuesStr := match[3] // The ANY(...) values

			// Parse the values inside ANY(...)
			values := parseAnyValues(valuesStr)

			// Create appropriate condition based on operator
			var condition Condition
			if operator == "=" {
				condition = &AnyArrayContainsAnyCondition{field: fieldName, values: values}
			} else { // !=
				condition = &AnyArrayNotContainsAnyCondition{field: fieldName, values: values}
			}

			// Add the condition to our list
			anyConditions = append(anyConditions, condition)

			// Replace the ANY expression with a placeholder in the filter string
			// This allows the SQL parser to work while we handle ANY conditions separately
			// For a complex filter like "ANY(Skills) = 'coding' AND Active = true", we need to ensure
			// that we don't break the structure by replacing parts of it
			filter = strings.Replace(filter, fullMatch, "true", 1)
		}
	}

	// Then replace the simpler ANY(field) = 'value' pattern
	anyFieldMatches := anyFieldRegex.FindAllStringSubmatch(filter, -1)
	for _, match := range anyFieldMatches {
		if len(match) >= 4 { // We need at least 4 elements
			fullMatch := match[0] // Complete match
			fieldName := match[1] // The field name
			operator := match[2]  // = or !=
			valueStr := match[3]  // The value to match

			// Remove quotes if present
			if strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'") {
				valueStr = valueStr[1 : len(valueStr)-1]
			}

			// Create appropriate condition based on operator
			var condition Condition
			if operator == "=" {
				condition = &AnyArrayContainsCondition{field: fieldName, value: valueStr}
			} else { // !=
				condition = &AnyArrayNotContainsCondition{field: fieldName, value: valueStr}
			}

			// Add the condition to our list
			anyConditions = append(anyConditions, condition)

			// Replace the ANY expression with a placeholder in the filter string
			filter = strings.Replace(filter, fullMatch, "true", 1)
		}
	}

	return filter, anyConditions, nil
}

// Implementation of the evaluate method for AnyArrayContainsCondition
func (c *AnyArrayContainsCondition) evaluate(item any) bool {
	// We need to get an array/slice field and check if any element equals the value
	// log.Printf("AnyArrayContainsCondition: checking if field %s contains value %s", c.field, c.value)
	values, found := getArrayFieldValues(item, c.field)
	// log.Printf("Field values found: %v, values: %v", found, values)

	if !found {
		return false
	}
	if len(values) == 0 {
		return false
	}

	for _, v := range values {
		log.Printf("Comparing '%s' with '%s'", v, c.value)
		if v == c.value {
			log.Printf("Match found!")
			return true
		}
	}
	return false
}

// parseAnyValues parses values from an ANY(...) expression
func parseAnyValues(valuesStr string) []string {
	var values []string

	// Split by commas, but handle quoted strings properly
	tokenizer := strings.NewReader(valuesStr)
	scanner := bufio.NewScanner(tokenizer)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		// Skip whitespace and commas
		start := 0
		for ; start < len(data) && (data[start] == ' ' || data[start] == '\t' || data[start] == ','); start++ {
		}

		if start >= len(data) {
			return len(data), nil, nil
		}

		// Handle quoted string
		if data[start] == '\'' {
			start++ // Skip opening quote
			pos := start
			for ; pos < len(data) && data[pos] != '\''; pos++ {
			}
			if pos >= len(data) {
				if atEOF {
					return len(data), data[start:pos], nil
				}
				return 0, nil, nil // Need more data
			}
			return pos + 1, data[start:pos], nil // +1 to consume the closing quote
		}

		// Non-quoted value
		pos := start
		for ; pos < len(data) && data[pos] != ','; pos++ {
		}
		return pos, data[start:pos], nil
	})

	for scanner.Scan() {
		val := scanner.Text()
		if val = strings.TrimSpace(val); val != "" {
			values = append(values, val)
		}
	}

	if len(values) == 0 {
		// Fallback method if the scanner didn't work
		rawValues := strings.SplitSeq(valuesStr, ",")
		for v := range rawValues {
			v = strings.TrimSpace(v)
			// Remove surrounding quotes if present
			if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
				v = v[1 : len(v)-1]
			}
			if v != "" {
				values = append(values, v)
			}
		}
	}

	return values
}
