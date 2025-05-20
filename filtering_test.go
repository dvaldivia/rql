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
	"log"
	"reflect"
	"testing"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Test struct for generic filtering tests
type TestPerson struct {
	ID         int
	Name       string
	Email      string
	Age        int
	Active     bool
	Department *Department
	Tags       []string   // Array field for ANY operator tests
	Skills     []string   // Another array field
	Scores     []int      // Array of integers
	Metadata   *TestMetadata // Nested object with array
}

type Department struct {
	Name     string
	Location string
}

type TestMetadata struct {
	Categories []string
}

func TestApplyFilter(t *testing.T) {
	// Test data
	people := []TestPerson{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 25, Active: true, 
			Department: &Department{Name: "Engineering", Location: "Building A"}, 
			Tags: []string{"frontend", "javascript", "react"}, 
			Skills: []string{"coding", "design"}, 
			Scores: []int{90, 85, 88},
			Metadata: &TestMetadata{Categories: []string{"developer", "ui"}}},
		{ID: 2, Name: "Bob", Email: "bob@example.com", Age: 30, Active: true, 
			Department: &Department{Name: "Engineering", Location: "Building B"}, 
			Tags: []string{"backend", "python", "go"}, 
			Skills: []string{"coding", "architecture"}, 
			Scores: []int{95, 92, 99},
			Metadata: &TestMetadata{Categories: []string{"developer", "backend"}}},
		{ID: 3, Name: "Charlie", Email: "charlie@example.com", Age: 35, Active: false, 
			Department: &Department{Name: "Marketing", Location: "Building C"}, 
			Tags: []string{"content", "strategy"}, 
			Skills: []string{"writing", "analysis"}, 
			Scores: []int{85, 80, 90},
			Metadata: &TestMetadata{Categories: []string{"marketing", "content"}}},
		{ID: 4, Name: "Dave", Email: "dave@alternative.com", Age: 40, Active: true, 
			Department: &Department{Name: "Sales", Location: "Building D"}, 
			Tags: []string{"enterprise", "relationships"}, 
			Skills: []string{"negotiation", "presentation"}, 
			Scores: []int{88, 92, 87},
			Metadata: &TestMetadata{Categories: []string{"sales", "account-management"}}},
		{ID: 5, Name: "Eve", Email: "eve@example.com", Age: 45, Active: false, 
			Department: &Department{Name: "HR", Location: "Building E"}, 
			Tags: []string{"recruiting", "training"}, 
			Skills: []string{"interviewing", "policy"}, 
			Scores: []int{91, 85, 89},
			Metadata: &TestMetadata{Categories: []string{"hr", "people"}}},
	}

	tests := []struct {
		name      string
		filter    string
		wantCount int
		wantIDs   []int
		wantErr   bool
	}{
		{
			name:      "empty filter",
			filter:    "",
			wantCount: 5,
			wantIDs:   []int{1, 2, 3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "filter by name",
			filter:    "Name='Alice'",
			wantCount: 1,
			wantIDs:   []int{1},
			wantErr:   false,
		},
		{
			name:      "filter by name not equal",
			filter:    "Name!='Alice'",
			wantCount: 4,
			wantIDs:   []int{2, 3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "filter by active status",
			filter:    "Active=true",
			wantCount: 3,
			wantIDs:   []int{1, 2, 4},
			wantErr:   false,
		},
		{
			name:      "filter by age",
			filter:    "Age='35'",
			wantCount: 1,
			wantIDs:   []int{3},
			wantErr:   false,
		},
		{
			name:      "filter by age greater or equal than",
			filter:    "Age>=40",
			wantCount: 2,
			wantIDs:   []int{4, 5},
			wantErr:   false,
		},
		{
			name:      "filter by age greater or equal than",
			filter:    "Age>='40'",
			wantCount: 2,
			wantIDs:   []int{4, 5},
			wantErr:   false,
		},
		{
			name:      "filter with AND",
			filter:    "Department.Name='Engineering' AND Active='true'",
			wantCount: 2,
			wantIDs:   []int{1, 2},
			wantErr:   false,
		},
		{
			name:      "filter with OR",
			filter:    "Name='Alice' OR Name='Bob'",
			wantCount: 2,
			wantIDs:   []int{1, 2},
			wantErr:   false,
		},
		{
			name:      "complex filter with nested conditions",
			filter:    "(Age='25' OR Age='30') AND Department.Name='Engineering'",
			wantCount: 2,
			wantIDs:   []int{1, 2},
			wantErr:   false,
		},
		{
			name:      "not equal filter",
			filter:    "Department.Name!='Engineering'",
			wantCount: 3,
			wantIDs:   []int{3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "LIKE filter",
			filter:    "Email LIKE '%alice%'",
			wantCount: 1,
			wantIDs:   []int{1},
			wantErr:   false,
		},
		{
			name:      "ILIKE filter",
			filter:    "Department.Location ILIKE '%building%'",
			wantCount: 5,
			wantIDs:   []int{1, 2, 3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "ILIKE filter lowercase",
			filter:    "Department.Location ILIKE '% d%'", // Match 'Building D' but not the 'd' in 'building'
			wantCount: 1,
			wantIDs:   []int{4},
			wantErr:   false,
		},
		{
			name:      "LIKE filter case-sensity",
			filter:    "Email LIKE '%@alternative.com'",
			wantCount: 1,
			wantIDs:   []int{4},
			wantErr:   false,
		},
		{
			name:      "LIKE all",
			filter:    "Email LIKE '%'",
			wantCount: 5,
			wantIDs:   []int{1, 2, 3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "LIKE filter case-sensity, field name lowercase",
			filter:    "email LIKE '%@alternative.com'",
			wantCount: 1,
			wantIDs:   []int{4},
			wantErr:   false,
		},
		{
			name:      "non-existent field",
			filter:    "NonExistentField='value'",
			wantCount: 0,
			wantIDs:   []int{},
			wantErr:   false,
		},
		{
			name:      "invalid filter",
			filter:    "quack?",
			wantCount: 0,
			wantIDs:   []int{},
			wantErr:   true,
		},
		{
			name:      "LIKE pattern with wildcard at start",
			filter:    "Email LIKE '%example.com'",
			wantCount: 4,
			wantIDs:   []int{1, 2, 3, 5},
			wantErr:   false,
		},
		{
			name:      "LIKE pattern with wildcard in middle",
			filter:    "Email LIKE 'a%com'",
			wantCount: 1,
			wantIDs:   []int{1},
			wantErr:   false,
		},
		{
			name:      "LIKE pattern matching substring",
			filter:    "Department.Location LIKE '%Building%'",
			wantCount: 5,
			wantIDs:   []int{1, 2, 3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "ANY operator with single value equal",
			filter:    "ANY(Tags) = 'javascript'",
			wantCount: 1,
			wantIDs:   []int{1},
			wantErr:   false,
		},
		{
			name:      "ANY operator with single value not equal",
			filter:    "ANY(Tags) != 'javascript'",
			wantCount: 4,
			wantIDs:   []int{2, 3, 4, 5},
			wantErr:   false,
		},
		{
			name:      "ANY operator with multiple values",
			filter:    "ANY(Tags) = ANY('python', 'content')",
			wantCount: 2,
			wantIDs:   []int{2, 3},
			wantErr:   false,
		},
		{
			name:      "ANY operator with multiple values not equal",
			filter:    "ANY(Tags) != ANY('python', 'content')",
			wantCount: 3,
			wantIDs:   []int{1, 4, 5},
			wantErr:   false,
		},
		{
			name:      "ANY operator with integer array",
			filter:    "ANY(Scores) = '90'",
			wantCount: 2,
			wantIDs:   []int{1, 3},
			wantErr:   false,
		},
		{
			name:      "ANY operator with nested field",
			filter:    "ANY(Metadata.Categories) = 'developer'",
			wantCount: 2,
			wantIDs:   []int{1, 2},
			wantErr:   false,
		},
		{
			name:      "ANY operator with nested field and multiple values",
			filter:    "ANY(Metadata.Categories) = ANY('marketing', 'hr')",
			wantCount: 2,
			wantIDs:   []int{3, 5},
			wantErr:   false,
		},
		{
			name:      "Complex filter with ANY and other conditions",
			filter:    "ANY(Skills) = 'coding' AND Active = true",
			wantCount: 2,
			wantIDs:   []int{1, 2},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyFilter(tt.filter, people)

			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(result.Items) != tt.wantCount {
				t.Errorf("ApplyFilter() got %d items, want %d", len(result.Items), tt.wantCount)
			}

			// Check if we got the expected IDs
			gotIDs := make([]int, 0, len(result.Items))
			for _, person := range result.Items {
				gotIDs = append(gotIDs, person.ID)
			}

			if !reflect.DeepEqual(gotIDs, tt.wantIDs) {
				t.Errorf("ApplyFilter() got IDs = %v, want %v", gotIDs, tt.wantIDs)
			}

			// In tests without pagination, Count should equal the number of Items
			if tt.name != "with pagination options" && result.Count != len(result.Items) {
				t.Errorf("ApplyFilter() Count = %d, expected to equal len(Items) = %d", result.Count, len(result.Items))
			}
		})
	}
}

// TestPaginationOptions tests the pagination functionality (limit and offset) of ApplyFilter
func TestPaginationOptions(t *testing.T) {
	// Test data
	people := []TestPerson{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 25, Active: true, Department: &Department{Name: "Engineering", Location: "Building A"}},
		{ID: 2, Name: "Bob", Email: "bob@example.com", Age: 30, Active: true, Department: &Department{Name: "Engineering", Location: "Building B"}},
		{ID: 3, Name: "Charlie", Email: "charlie@example.com", Age: 35, Active: false, Department: &Department{Name: "Marketing", Location: "Building C"}},
		{ID: 4, Name: "Dave", Email: "dave@alternative.com", Age: 40, Active: true, Department: &Department{Name: "Sales", Location: "Building D"}},
		{ID: 5, Name: "Eve", Email: "eve@example.com", Age: 45, Active: false, Department: &Department{Name: "HR", Location: "Building E"}},
	}

	tests := []struct {
		name           string
		filter         string
		options        FilterOptions
		wantCount      int // Expected number of items after pagination
		wantTotalCount int // Expected total count before pagination
		wantIDs        []int
		wantErr        bool
	}{
		{
			name:           "limit only",
			filter:         "", // No filter, just limit
			options:        FilterOptions{Limit: 3, Offset: 0},
			wantCount:      3,
			wantTotalCount: 5, // All 5 people match (no filter), but only 3 returned due to limit
			wantIDs:        []int{1, 2, 3},
			wantErr:        false,
		},
		{
			name:           "offset only",
			filter:         "", // No filter, just offset
			options:        FilterOptions{Limit: 0, Offset: 2},
			wantCount:      3,
			wantTotalCount: 5, // All 5 people match (no filter), but only 3 returned due to offset
			wantIDs:        []int{3, 4, 5},
			wantErr:        false,
		},
		{
			name:           "limit and offset combined",
			filter:         "", // No filter, both limit and offset
			options:        FilterOptions{Limit: 2, Offset: 1},
			wantCount:      2,
			wantTotalCount: 5, // All 5 people match (no filter), but only 2 returned due to limit+offset
			wantIDs:        []int{2, 3},
			wantErr:        false,
		},
		{
			name:           "filter with pagination",
			filter:         "Age >= 30",
			options:        FilterOptions{Limit: 2, Offset: 1},
			wantCount:      2,
			wantTotalCount: 4,           // 4 people match the filter (Age >= 30), but only 2 returned due to limit+offset
			wantIDs:        []int{3, 4}, // Ages 30, 35, 40, 45 sorted by ID, offset 1, limit 2 gives IDs 3, 4
			wantErr:        false,
		},
		{
			name:           "pagination beyond available items",
			filter:         "",
			options:        FilterOptions{Limit: 10, Offset: 5},
			wantCount:      0,
			wantTotalCount: 5, // All 5 people match (no filter), but none returned due to offset beyond available
			wantIDs:        []int{},
			wantErr:        false,
		},
		{
			name:           "zero limit returns all items after offset",
			filter:         "",
			options:        FilterOptions{Limit: 0, Offset: 3},
			wantCount:      2,
			wantTotalCount: 5, // All 5 people match (no filter), but only 2 returned due to offset
			wantIDs:        []int{4, 5},
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyFilter(tt.filter, people, tt.options)

			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(result.Items) != tt.wantCount {
				t.Errorf("ApplyFilter() got %d items, want %d", len(result.Items), tt.wantCount)
			}

			if result.Count != tt.wantTotalCount {
				t.Errorf("ApplyFilter() got total count %d, want %d", result.Count, tt.wantTotalCount)
			}

			// Check if we got the expected IDs
			gotIDs := make([]int, 0, len(result.Items))
			for _, person := range result.Items {
				gotIDs = append(gotIDs, person.ID)
			}

			if !reflect.DeepEqual(gotIDs, tt.wantIDs) {
				t.Errorf("ApplyFilter() got IDs = %v, want %v", gotIDs, tt.wantIDs)
			}
		})
	}
}

// TestCountResults specifically tests the Count field behavior in the Result struct
// focusing on ensuring Count accurately reflects total matches regardless of pagination
func TestCountResults(t *testing.T) {
	// Test data
	people := []TestPerson{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 25, Active: true},
		{ID: 2, Name: "Bob", Email: "bob@example.com", Age: 30, Active: true},
		{ID: 3, Name: "Charlie", Email: "charlie@example.com", Age: 35, Active: false},
		{ID: 4, Name: "Dave", Email: "dave@alternative.com", Age: 40, Active: true},
		{ID: 5, Name: "Eve", Email: "eve@example.com", Age: 45, Active: false},
		{ID: 6, Name: "Frank", Email: "frank@example.com", Age: 50, Active: true},
		{ID: 7, Name: "Grace", Email: "grace@example.com", Age: 55, Active: true},
		{ID: 8, Name: "Helen", Email: "helen@example.com", Age: 60, Active: false},
		{ID: 9, Name: "Ivan", Email: "ivan@example.com", Age: 65, Active: true},
		{ID: 10, Name: "Jane", Email: "jane@alternative.com", Age: 70, Active: false},
	}

	tests := []struct {
		name           string
		filter         string
		options        FilterOptions
		wantCount      int // Expected number of items returned (after pagination)
		wantTotalCount int // Expected total number of matches (before pagination)
	}{
		{
			name:           "no pagination - count equals items",
			filter:         "Age > 40",
			options:        FilterOptions{}, // Default options (no limit, no offset)
			wantCount:      6,               // People with Age > 40: IDs 5-10
			wantTotalCount: 6,               // Total matches should equal returned items
		},
		{
			name:           "with limit - count greater than items",
			filter:         "Age > 40",
			options:        FilterOptions{Limit: 3}, // Only return first 3
			wantCount:      3,                       // Only 3 items returned due to limit
			wantTotalCount: 6,                       // But 6 total matches (IDs 5-10)
		},
		{
			name:           "with offset - count greater than items",
			filter:         "Age > 40",
			options:        FilterOptions{Offset: 4}, // Skip first 4
			wantCount:      2,                        // Only 2 items returned after offset
			wantTotalCount: 6,                        // But 6 total matches (IDs 5-10)
		},
		{
			name:           "with limit and offset - count greater than items",
			filter:         "Age > 30",
			options:        FilterOptions{Limit: 3, Offset: 2}, // Skip 2, limit to 3
			wantCount:      3,                                  // 3 items returned
			wantTotalCount: 8,                                  // But 8 total matches (IDs 3-10)
		},
		{
			name:           "complex filter - count accurate",
			filter:         "Age > 40 AND Active = true",
			options:        FilterOptions{Limit: 2},
			wantCount:      2, // Only 2 returned due to limit
			wantTotalCount: 3, // But 3 total matches (IDs 6, 7, 9)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyFilter(tt.filter, people, tt.options)
			if err != nil {
				t.Fatalf("ApplyFilter() unexpected error = %v", err)
			}

			if len(result.Items) != tt.wantCount {
				t.Errorf("ApplyFilter() returned %d items, want %d", len(result.Items), tt.wantCount)
			}

			if result.Count != tt.wantTotalCount {
				t.Errorf("ApplyFilter() Count = %d, want %d", result.Count, tt.wantTotalCount)
			}

			// Verify the Count is always >= number of items returned
			if result.Count < len(result.Items) {
				t.Errorf("ApplyFilter() Count (%d) is less than number of items (%d), which should never happen",
					result.Count, len(result.Items))
			}
		})
	}
}

type MsgPackStruct struct {
	ID        string `json:"id" msg:"i"`
	Path      string `json:"path" msg:"p"`
	SomeIndex int    `json:"someIndex" msg:"si"`
}

func TestMsgPack(t *testing.T) {
	// Test data
	testData := []MsgPackStruct{
		{
			ID:        "123",
			Path:      "/abc/def",
			SomeIndex: 5,
		},
		{
			ID:        "456",
			Path:      "/abc/zzz",
			SomeIndex: 5,
		},
		{
			ID:        "457",
			Path:      "/abc/daz",
			SomeIndex: 6,
		},
	}
	tests := []struct {
		name           string
		filter         string
		options        FilterOptions
		wantCount      int // Expected number of items returned (after pagination)
		wantTotalCount int // Expected total number of matches (before pagination)
	}{
		{
			name:           "single record",
			filter:         "id = 123",
			options:        FilterOptions{}, // Default options (no limit, no offset)
			wantCount:      1,               // People with Age > 40: IDs 5-10
			wantTotalCount: 1,               // Total matches should equal returned items
		},
		{
			name:           "some index equal",
			filter:         "someIndex = 5",
			options:        FilterOptions{}, // Default options (no limit, no offset)
			wantCount:      2,               // People with Age > 40: IDs 5-10
			wantTotalCount: 2,               // Total matches should equal returned items
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyFilter(tt.filter, testData, tt.options)
			if err != nil {
				t.Fatalf("ApplyFilter() unexpected error = %v", err)
			}

			if len(result.Items) != tt.wantCount {
				t.Errorf("ApplyFilter() returned %d items, want %d", len(result.Items), tt.wantCount)
			}

			if result.Count != tt.wantTotalCount {
				t.Errorf("ApplyFilter() Count = %d, want %d", result.Count, tt.wantTotalCount)
			}

			// Verify the Count is always >= number of items returned
			if result.Count < len(result.Items) {
				t.Errorf("ApplyFilter() Count (%d) is less than number of items (%d), which should never happen",
					result.Count, len(result.Items))
			}
		})
	}
}
