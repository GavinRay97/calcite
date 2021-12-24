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
package org.apache.calcite.plan

import org.apache.calcite.DataContext

/**
 * DataContext for evaluating a RexExpression.
 */
class VisitorDataContext(@Nullable values: Array<Object?>) : DataContext {
    @Nullable
    private val values: Array<Object?>

    init {
        this.values = values
    }

    @get:Override
    val rootSchema: SchemaPlus
        get() {
            throw RuntimeException("Unsupported")
        }

    @get:Override
    val typeFactory: JavaTypeFactory
        get() {
            throw RuntimeException("Unsupported")
        }

    @get:Override
    val queryProvider: QueryProvider
        get() {
            throw RuntimeException("Unsupported")
        }

    @Override
    @Nullable
    operator fun get(name: String): Object? {
        return if (name.equals("inputRecord")) {
            values
        } else {
            null
        }
    }

    companion object {
        private val LOGGER: CalciteLogger =
            CalciteLogger(LoggerFactory.getLogger(VisitorDataContext::class.java.getName()))

        @Nullable
        fun of(targetRel: RelNode, queryRel: LogicalFilter): DataContext {
            return of(targetRel.getRowType(), queryRel.getCondition())
        }

        @Nullable
        fun of(rowType: RelDataType, rex: RexNode): DataContext? {
            val size: Int = rowType.getFieldList().size()
            val operands: List<RexNode> = (rex as RexCall).getOperands()
            val firstOperand: RexNode = operands[0]
            val secondOperand: RexNode = operands[1]
            val value: Pair<Integer, *>? = getValue(firstOperand, secondOperand)
            return if (value != null) {
                @Nullable val values: Array<Object?> = arrayOfNulls<Object>(size)
                val index: Int = value.getKey()
                values[index] = value.getValue()
                VisitorDataContext(values)
            } else {
                null
            }
        }

        @Nullable
        fun of(
            rowType: RelDataType,
            usageList: List<Pair<RexInputRef?, out RexNode?>?>
        ): DataContext? {
            val size: Int = rowType.getFieldList().size()
            @Nullable val values: Array<Object?> = arrayOfNulls<Object>(size)
            for (elem in usageList) {
                val value: Pair<Integer, *>? = getValue(elem.getKey(), elem.getValue())
                if (value == null) {
                    LOGGER.warn(
                        "{} is not handled for {} for checking implication",
                        elem.getKey(), elem.getValue()
                    )
                    return null
                }
                val index: Int = value.getKey()
                values[index] = value.getValue()
            }
            return VisitorDataContext(values)
        }

        @Nullable
        operator fun getValue(
            @Nullable inputRef: RexNode?, @Nullable literal: RexNode?
        ): Pair<Integer, out Object?>? {
            var inputRef: RexNode? = inputRef
            var literal: RexNode? = literal
            inputRef = if (inputRef == null) null else RexUtil.removeCast(inputRef)
            literal = if (literal == null) null else RexUtil.removeCast(literal)
            if (inputRef is RexInputRef
                && literal is RexLiteral
            ) {
                val index: Int = (inputRef as RexInputRef?).getIndex()
                val rexLiteral: RexLiteral? = literal as RexLiteral?
                val type: RelDataType = inputRef.getType()
                if (type.getSqlTypeName() == null) {
                    LOGGER.warn("{} returned null SqlTypeName", inputRef.toString())
                    return null
                }
                return when (type.getSqlTypeName()) {
                    INTEGER -> Pair.of(index, rexLiteral.getValueAs(Integer::class.java))
                    DOUBLE -> Pair.of(index, rexLiteral.getValueAs(Double::class.java))
                    REAL -> Pair.of(index, rexLiteral.getValueAs(Float::class.java))
                    BIGINT -> Pair.of(index, rexLiteral.getValueAs(Long::class.java))
                    SMALLINT -> Pair.of(index, rexLiteral.getValueAs(Short::class.java))
                    TINYINT -> Pair.of(index, rexLiteral.getValueAs(Byte::class.java))
                    DECIMAL -> Pair.of(index, rexLiteral.getValueAs(BigDecimal::class.java))
                    DATE, TIME -> Pair.of(index, rexLiteral.getValueAs(Integer::class.java))
                    TIMESTAMP -> Pair.of(index, rexLiteral.getValueAs(Long::class.java))
                    CHAR -> Pair.of(index, rexLiteral.getValueAs(Character::class.java))
                    VARCHAR -> Pair.of(index, rexLiteral.getValueAs(String::class.java))
                    else -> {
                        // TODO: Support few more supported cases
                        val value: Comparable = rexLiteral.getValue()
                        LOGGER.warn(
                            "{} for value of class {} is being handled in default way",
                            type.getSqlTypeName(), if (value == null) null else value.getClass()
                        )
                        if (value is NlsString) {
                            Pair.of(index, (value as NlsString).getValue())
                        } else {
                            Pair.of(index, value)
                        }
                    }
                }
            }

            // Unsupported Arguments
            return null
        }
    }
}
