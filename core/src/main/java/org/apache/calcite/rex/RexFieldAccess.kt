/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rex

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlKind
import com.google.common.base.Preconditions

/**
 * Access to a field of a row-expression.
 *
 *
 * You might expect to use a `RexFieldAccess` to access columns of
 * relational tables, for example, the expression `emp.empno` in the
 * query
 *
 * <blockquote>
 * <pre>SELECT emp.empno FROM emp</pre>
</blockquote> *
 *
 *
 * but there is a specialized expression [RexInputRef] for this
 * purpose. So in practice, `RexFieldAccess` is usually used to
 * access fields of correlating variables, for example the expression
 * `emp.deptno` in
 *
 * <blockquote>
 * <pre>SELECT ename
 * FROM dept
 * WHERE EXISTS (
 * SELECT NULL
 * FROM emp
 * WHERE emp.deptno = dept.deptno
 * AND gender = 'F')</pre>
</blockquote> *
 */
class RexFieldAccess internal constructor(
    expr: RexNode,
    field: RelDataTypeField
) : RexNode() {
    //~ Instance fields --------------------------------------------------------
    private val expr: RexNode
    private val field: RelDataTypeField

    //~ Constructors -----------------------------------------------------------
    init {
        checkValid(expr, field)
        this.expr = expr
        this.field = field
        this.digest = expr.toString() + "." + field.getName()
    }

    fun getField(): RelDataTypeField {
        return field
    }

    @get:Override
    val type: RelDataType
        get() = field.getType()

    @get:Override
    val kind: SqlKind
        get() = SqlKind.FIELD_ACCESS

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitFieldAccess(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitFieldAccess(this, arg)
    }

    /**
     * Returns the expression whose field is being accessed.
     */
    val referenceExpr: RexNode
        get() = expr

    @Override
    override fun equals(@Nullable o: Object?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || getClass() !== o.getClass()) {
            return false
        }
        val that = o as RexFieldAccess
        return field.equals(that.field) && expr.equals(that.expr)
    }

    @Override
    override fun hashCode(): Int {
        var result: Int = expr.hashCode()
        result = 31 * result + field.hashCode()
        return result
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        private fun checkValid(expr: RexNode, field: RelDataTypeField) {
            val exprType: RelDataType = expr.getType()
            val fieldIdx: Int = field.getIndex()
            Preconditions.checkArgument(
                fieldIdx >= 0 && fieldIdx < exprType.getFieldList().size() && exprType.getFieldList().get(fieldIdx)
                    .equals(field),
                "Field $field does not exist for expression $expr"
            )
        }
    }
}
