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

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlQuantifyOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeName
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import java.util.List
import java.util.Objects

/**
 * Scalar expression that represents an IN, EXISTS or scalar sub-query.
 */
class RexSubQuery private constructor(
    type: RelDataType, op: SqlOperator,
    operands: ImmutableList<RexNode?>, rel: RelNode
) : RexCall(type, op, operands) {
    val rel: RelNode

    init {
        this.rel = rel
    }

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitSubQuery(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitSubQuery(this, arg)
    }

    @Override
    protected fun computeDigest(withType: Boolean): String {
        val sb = StringBuilder(op.getName())
        sb.append("(")
        for (operand in operands) {
            sb.append(operand)
            sb.append(", ")
        }
        sb.append("{\n")
        sb.append(RelOptUtil.toString(rel))
        sb.append("})")
        return sb.toString()
    }

    @Override
    fun clone(type: RelDataType, operands: List<RexNode?>?): RexSubQuery {
        return RexSubQuery(
            type, getOperator(),
            ImmutableList.copyOf(operands), rel
        )
    }

    fun clone(rel: RelNode): RexSubQuery {
        return RexSubQuery(type, getOperator(), operands, rel)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is RexSubQuery) {
            return false
        }
        val sq = obj as RexSubQuery
        return (op.equals(sq.op)
                && operands.equals(sq.operands)
                && rel.deepEquals(sq.rel))
    }

    @Override
    override fun hashCode(): Int {
        if (hash === 0) {
            hash = Objects.hash(op, operands, rel.deepHashCode())
        }
        return hash
    }

    companion object {
        /** Creates an IN sub-query.  */
        fun `in`(rel: RelNode, nodes: ImmutableList<RexNode?>): RexSubQuery {
            val type: RelDataType = type(rel, nodes)
            return RexSubQuery(type, SqlStdOperatorTable.IN, nodes, rel)
        }

        /** Creates a SOME sub-query.
         *
         *
         * There is no ALL. For `x comparison ALL (sub-query)` use instead
         * `NOT (x inverse-comparison SOME (sub-query))`.
         * If `comparison` is `>`
         * then `negated-comparison` is `<=`, and so forth.
         *
         *
         * Also =SOME is rewritten into IN  */
        fun some(
            rel: RelNode, nodes: ImmutableList<RexNode?>,
            op: SqlQuantifyOperator
        ): RexSubQuery {
            assert(op.kind === SqlKind.SOME)
            if (op === SqlStdOperatorTable.SOME_EQ) {
                return `in`(rel, nodes)
            }
            val type: RelDataType = type(rel, nodes)
            return RexSubQuery(type, op, nodes, rel)
        }

        fun type(rel: RelNode, nodes: ImmutableList<RexNode?>): RelDataType {
            assert(rel.getRowType().getFieldCount() === nodes.size())
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            var nullable = false
            for (node in nodes) {
                if (node.getType().isNullable()) {
                    nullable = true
                }
            }
            for (field in rel.getRowType().getFieldList()) {
                if (field.getType().isNullable()) {
                    nullable = true
                }
            }
            return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullable
            )
        }

        /** Creates an EXISTS sub-query.  */
        fun exists(rel: RelNode): RexSubQuery {
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val type: RelDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN)
            return RexSubQuery(
                type, SqlStdOperatorTable.EXISTS,
                ImmutableList.of(), rel
            )
        }

        /** Creates an UNIQUE sub-query.  */
        fun unique(rel: RelNode): RexSubQuery {
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val type: RelDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN)
            return RexSubQuery(
                type, SqlStdOperatorTable.UNIQUE,
                ImmutableList.of(), rel
            )
        }

        /** Creates a scalar sub-query.  */
        fun scalar(rel: RelNode): RexSubQuery {
            val fieldList: List<RelDataTypeField> = rel.getRowType().getFieldList()
            if (fieldList.size() !== 1) {
                throw IllegalArgumentException()
            }
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val type: RelDataType = typeFactory.createTypeWithNullability(fieldList[0].getType(), true)
            return RexSubQuery(
                type, SqlStdOperatorTable.SCALAR_QUERY,
                ImmutableList.of(), rel
            )
        }

        /** Creates an ARRAY sub-query.  */
        fun array(rel: RelNode): RexSubQuery {
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val type: RelDataType = typeFactory.createArrayType(rel.getRowType(), -1L)
            return RexSubQuery(
                type, SqlStdOperatorTable.ARRAY_QUERY,
                ImmutableList.of(), rel
            )
        }

        /** Creates a MULTISET sub-query.  */
        fun multiset(rel: RelNode): RexSubQuery {
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val type: RelDataType = typeFactory.createMultisetType(rel.getRowType(), -1L)
            return RexSubQuery(
                type, SqlStdOperatorTable.MULTISET_QUERY,
                ImmutableList.of(), rel
            )
        }

        /** Creates a MAP sub-query.  */
        fun map(rel: RelNode): RexSubQuery {
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val rowType: RelDataType = rel.getRowType()
            Preconditions.checkArgument(
                rowType.getFieldCount() === 2,
                "MAP requires exactly two fields, got %s; row type %s",
                rowType.getFieldCount(), rowType
            )
            val fieldList: List<RelDataTypeField> = rowType.getFieldList()
            val type: RelDataType = typeFactory.createMapType(
                fieldList[0].getType(),
                fieldList[1].getType()
            )
            return RexSubQuery(
                type, SqlStdOperatorTable.MAP_QUERY,
                ImmutableList.of(), rel
            )
        }
    }
}
