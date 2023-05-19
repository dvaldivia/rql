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
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/doug-martin/goqu/v9"
)

type AstVisitor struct {
	expressions []goqu.Expression
}

func (v *AstVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {

	switch node := expr.(type) {
	case *tree.ComparisonExpr:
		//fmt.Println("hit->>", node)
		x := processComparison(node)
		v.expressions = append(v.expressions, x)
		return false, expr
	case *tree.AndExpr:
		aw := &AstVisitor{}
		_ = node.Walk(aw)
		v.expressions = append(v.expressions, goqu.And(aw.expressions...))
		return false, expr
	case *tree.OrExpr:
		aw := &AstVisitor{}
		_ = node.Walk(aw)
		v.expressions = append(v.expressions, goqu.Or(aw.expressions...))
		return false, expr

	}
	return true, expr
}

func (v *AstVisitor) VisitPost(expr tree.Expr) (newNode tree.Expr) {
	//fmt.Printf("post-%T - %s \n", expr, expr.String())
	return expr
}
