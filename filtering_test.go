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
	"github.com/doug-martin/goqu/v9"
	"log"
	"reflect"
	"testing"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestParseFilter(t *testing.T) {
	type args struct {
		rawFilters string
	}
	tests := []struct {
		name    string
		args    args
		want    []goqu.Expression
		wantErr bool
	}{
		{
			name: "nothing",
			args: args{
				rawFilters: "",
			},
		},
		{
			name: "basic",
			args: args{
				rawFilters: "field1='value1'",
			},
			want: []goqu.Expression{
				goqu.C("field1").Eq("value1"),
			},
		},
		{
			name: "x1!=v1",
			args: args{
				rawFilters: "field1!='value1'",
			},
			want: []goqu.Expression{
				goqu.C("field1").Neq("value1"),
			},
		},
		{
			name: "x1 LIKE '%v1%'",
			args: args{
				rawFilters: "field1 LIKE '%value1%'",
			},
			want: []goqu.Expression{
				goqu.C("field1").Like("%value1%"),
			},
		},
		{
			name: "x1 ILIKE '%v1%'",
			args: args{
				rawFilters: "field1 ILIKE '%value1%'",
			},
			want: []goqu.Expression{
				goqu.C("field1").ILike("%value1%"),
			},
		},
		{
			name: "and statement",
			args: args{
				rawFilters: "field1='value1' AND field2='value2'",
			},
			want: []goqu.Expression{
				goqu.C("field1").Eq("value1"),
				goqu.C("field2").Eq("value2"),
			},
		},
		{
			name: "or statement",
			args: args{
				rawFilters: "field1='value1' OR field2='value2'",
			},
			want: []goqu.Expression{
				goqu.Or([]goqu.Expression{
					goqu.C("field1").Eq("value1"),
					goqu.C("field2").Eq("value2"),
				}...),
			},
		},
		{
			name: "and x and (y=1 or z=2) statement",
			args: args{
				rawFilters: "field1='value1' AND (field2='value2' OR field3='value3')",
			},
			want: []goqu.Expression{
				goqu.C("field1").Eq("value1"),
				goqu.Or([]goqu.Expression{
					goqu.C("field2").Eq("value2"),
					goqu.C("field3").Eq("value3"),
				}...),
			},
		},
		{
			name: " x1=v1 or y2=v2 and z3=v3 statement",
			args: args{
				rawFilters: "field1='value1' OR field2='value2' AND field3='value3'",
			},
			want: []goqu.Expression{
				goqu.Or([]goqu.Expression{
					goqu.C("field1").Eq("value1"),
					goqu.And([]goqu.Expression{
						goqu.C("field2").Eq("value2"),
						goqu.C("field3").Eq("value3"),
					}...),
				}...),
			},
		},
		{
			name: " x=0 and y=1 or z=2 statement",
			args: args{
				rawFilters: "field1='value1' AND field2='value2' OR field3='value3'",
			},
			want: []goqu.Expression{
				goqu.Or([]goqu.Expression{
					goqu.And([]goqu.Expression{
						goqu.C("field1").Eq("value1"),
						goqu.C("field2").Eq("value2"),
					}...),
					goqu.C("field3").Eq("value3"),
				}...),
			},
		},
		{
			name: "and x and (y=1 and z=2) statement",
			args: args{
				rawFilters: "field1='value1' AND (field2='value2' AND field3='value3')",
			},
			want: []goqu.Expression{
				goqu.C("field1").Eq("value1"),
				goqu.And([]goqu.Expression{
					goqu.C("field2").Eq("value2"),
					goqu.C("field3").Eq("value3"),
				}...),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFilter(tt.args.rawFilters)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				fmt.Println("want-----")
				q := goqu.From("t").Where(tt.want...)
				fmt.Println(q.ToSQL())
				fmt.Println("got-----")
				r := goqu.From("t").Where(got...)
				fmt.Println(r.ToSQL())

				t.Errorf("ParseFilter() got = %v, want %v", got, tt.want)
			}
		})
	}
}
