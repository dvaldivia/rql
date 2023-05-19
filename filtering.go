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
	"github.com/doug-martin/goqu/v9"
	"log"
	"net/url"
)

func ParseFilter(rawFilters string) ([]goqu.Expression, error) {
	if rawFilters == "" {
		var res []goqu.Expression
		return res, nil
	}
	rawFilters = url.QueryEscape(rawFilters)
	var err error
	rawFilters, err = url.QueryUnescape(rawFilters)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	pseudoQuery := fmt.Sprintf("SELECT * FROM t WHERE %s", rawFilters)
	queryTree, err := parser.Parse(pseudoQuery)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	var expr parser.Statements

	var whereExprs []goqu.Expression

	whereFinder := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			if whereNode, ok := node.(*tree.Where); ok {
				//log.Printf("node type %T", node)
				whereRes := parseWhere(whereNode)
				whereExprs = append(whereExprs, whereRes...)
				return true
			}
			return false
		},
	}

	_, _ = whereFinder.Walk(queryTree, nil)

	w := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			log.Printf("node type %T", node)
			log.Printf("===> %+v", node.(parser.Statement))
			if sq, ok := node.(*tree.AndExpr); ok {
				log.Printf("AND %T", sq)
				//andExprs, err := visitAndExpr(sq)
				//whereExprs = append(whereExprs, andExprs...)
				//if err != nil {
				//	return true
				//}
				return false
			} else if sq, ok := node.(*tree.OrExpr); ok {
				log.Printf("OR %T", sq)
				//orExpr, err := visitOrExpr(sq)
				//if err != nil {
				//	return true
				//}
				//whereExprs = append(whereExprs, orExpr)
				return false
			} else if sq, ok := node.(*tree.ComparisonExpr); ok {
				log.Printf("REST %T", sq)
				//finalExpr := expressionForNode(sq)
				//whereExprs = append(whereExprs, finalExpr)
				return false
			}
			return false
		},
	}

	ok, err := w.Walk(expr, nil)
	if err != nil {
		log.Println(err)
	}
	if !ok {
		log.Println("not ok?")
	}

	return whereExprs, nil
}

func parseWhere(whereNode *tree.Where) []goqu.Expression {
	var expressions []goqu.Expression
	switch node := whereNode.Expr.(type) {
	case *tree.ComparisonExpr:
		gstmt := processComparison(node)
		expressions = append(expressions, gstmt)
	case *tree.AndExpr:
		aw := &AstVisitor{}
		_ = node.Walk(aw)
		expressions = append(expressions, aw.expressions...)
	case *tree.OrExpr:
		aw := &AstVisitor{}
		_ = node.Walk(aw)
		expressions = append(expressions, goqu.Or(aw.expressions...))
	}

	return expressions
}

func processComparison(node *tree.ComparisonExpr) goqu.Expression {
	var gstmt goqu.Expression
	colName := node.Left.String()
	var value string
	if valStr, ok := node.Right.(*tree.StrVal); ok {
		value = valStr.RawString()
	}

	switch node.Operator {
	case tree.NE:
		gstmt = goqu.C(colName).Neq(value)
	case tree.Like:
		gstmt = goqu.C(colName).Like(value)
	case tree.ILike:
		gstmt = goqu.C(colName).ILike(value)
	default:
		gstmt = goqu.C(colName).Eq(value)
	}
	return gstmt
}
