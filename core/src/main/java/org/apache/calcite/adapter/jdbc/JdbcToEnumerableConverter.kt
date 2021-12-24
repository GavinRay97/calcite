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
package org.apache.calcite.adapter.jdbc

import org.apache.calcite.DataContext

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
class JdbcToEnumerableConverter(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    input: RelNode?
) : ConverterImpl(cluster, ConventionTraitDef.INSTANCE, traits, input), EnumerableRel {
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return JdbcToEnumerableConverter(
            getCluster(), traitSet, sole(inputs)
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner?,
        mq: RelMetadataQuery?
    ): RelOptCost? {
        val cost: RelOptCost = super.computeSelfCost(planner, mq) ?: return null
        return cost.multiplyBy(.1)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        // Generate:
        //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
        val builder0 = BlockBuilder(false)
        val child: JdbcRel = getInput() as JdbcRel
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM)
        )
        val jdbcConvention: JdbcConvention = requireNonNull(
            child.getConvention()
        ) { "child.getConvention() is null for $child" } as JdbcConvention
        val sqlString: SqlString = generateSql(jdbcConvention.dialect)
        val sql: String = sqlString.getSql()
        if (CalciteSystemProperty.DEBUG.value()) {
            System.out.println("[$sql]")
        }
        Hook.QUERY_PLAN.run(sql)
        val sql_: Expression = builder0.append("sql", Expressions.constant(sql))
        val fieldCount: Int = getRowType().getFieldCount()
        val builder = BlockBuilder()
        val resultSet_: ParameterExpression = Expressions.parameter(
            Modifier.FINAL, ResultSet::class.java,
            builder.newName("resultSet")
        )
        val calendarPolicy: SqlDialect.CalendarPolicy = jdbcConvention.dialect.getCalendarPolicy()
        val calendar_: Expression?
        calendar_ = when (calendarPolicy) {
            LOCAL -> builder0.append(
                "calendar",
                Expressions.call(
                    Calendar::class.java, "getInstance",
                    getTimeZoneExpression(implementor)
                )
            )
            else -> null
        }
        if (fieldCount == 1) {
            val value_: ParameterExpression = Expressions.parameter(Object::class.java, builder.newName("value"))
            builder.add(Expressions.declare(Modifier.FINAL, value_, null))
            generateGet(
                implementor, physType, builder, resultSet_, 0, value_,
                calendar_, calendarPolicy
            )
            builder.add(Expressions.return_(null, value_))
        } else {
            val values_: Expression = builder.append(
                "values",
                Expressions.newArrayBounds(
                    Object::class.java, 1,
                    Expressions.constant(fieldCount)
                )
            )
            for (i in 0 until fieldCount) {
                generateGet(
                    implementor, physType, builder, resultSet_, i,
                    Expressions.arrayIndex(values_, Expressions.constant(i)),
                    calendar_, calendarPolicy
                )
            }
            builder.add(
                Expressions.return_(null, values_)
            )
        }
        val e_: ParameterExpression = Expressions.parameter(SQLException::class.java, builder.newName("e"))
        val rowBuilderFactory_: Expression = builder0.append(
            "rowBuilderFactory",
            Expressions.lambda(
                Expressions.block(
                    Expressions.return_(
                        null,
                        Expressions.lambda(
                            Expressions.block(
                                Expressions.tryCatch(
                                    builder.toBlock(),
                                    Expressions.catch_(
                                        e_,
                                        Expressions.throw_(
                                            Expressions.new_(
                                                RuntimeException::class.java,
                                                e_
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                ),
                resultSet_
            )
        )
        val enumerable: Expression
        enumerable = if (sqlString.getDynamicParameters() != null
            && !sqlString.getDynamicParameters().isEmpty()
        ) {
            val preparedStatementConsumer_: Expression = builder0.append(
                "preparedStatementConsumer",
                Expressions.call(
                    BuiltInMethod.CREATE_ENRICHER.method,
                    Expressions.newArrayInit(
                        Integer::class.java, 1,
                        toIndexesTableExpression(sqlString)
                    ),
                    DataContext.ROOT
                )
            )
            builder0.append(
                "enumerable",
                Expressions.call(
                    BuiltInMethod.RESULT_SET_ENUMERABLE_OF_PREPARED.method,
                    Schemas.unwrap(jdbcConvention.expression, DataSource::class.java),
                    sql_,
                    rowBuilderFactory_,
                    preparedStatementConsumer_
                )
            )
        } else {
            builder0.append(
                "enumerable",
                Expressions.call(
                    BuiltInMethod.RESULT_SET_ENUMERABLE_OF.method,
                    Schemas.unwrap(jdbcConvention.expression, DataSource::class.java),
                    sql_,
                    rowBuilderFactory_
                )
            )
        }
        builder0.add(
            Expressions.statement(
                Expressions.call(
                    enumerable,
                    BuiltInMethod.RESULT_SET_ENUMERABLE_SET_TIMEOUT.method,
                    DataContext.ROOT
                )
            )
        )
        builder0.add(
            Expressions.return_(null, enumerable)
        )
        return implementor.result(physType, builder0.toBlock())
    }

    private fun generateSql(dialect: SqlDialect): SqlString {
        val jdbcImplementor = JdbcImplementor(
            dialect,
            getCluster().getTypeFactory() as JavaTypeFactory
        )
        val result: JdbcImplementor.Result = jdbcImplementor.visitInput(this, 0)
        return result.asStatement().toSqlString(dialect)
    }

    companion object {
        private fun toIndexesTableExpression(sqlString: SqlString): List<ConstantExpression> {
            return requireNonNull(
                sqlString.getDynamicParameters()
            ) { "sqlString.getDynamicParameters() is null for $sqlString" }.stream()
                .map(Expressions::constant)
                .collect(Collectors.toList())
        }

        private fun getTimeZoneExpression(
            implementor: EnumerableRelImplementor
        ): UnaryExpression {
            return Expressions.convert_(
                Expressions.call(
                    implementor.getRootExpression(),
                    "get",
                    Expressions.constant("timeZone")
                ),
                TimeZone::class.java
            )
        }

        private fun generateGet(
            implementor: EnumerableRelImplementor,
            physType: PhysType, builder: BlockBuilder, resultSet_: ParameterExpression,
            i: Int, target: Expression, @Nullable calendar_: Expression?,
            calendarPolicy: SqlDialect.CalendarPolicy
        ) {
            val primitive: Primitive = Primitive.ofBoxOr(physType.fieldClass(i))
            val fieldType: RelDataType = physType.getRowType().getFieldList().get(i).getType()
            val dateTimeArgs: List<Expression> = ArrayList()
            dateTimeArgs.add(Expressions.constant(i + 1))
            var sqlTypeName: SqlTypeName = fieldType.getSqlTypeName()
            var offset = false
            when (calendarPolicy) {
                LOCAL -> {
                    assert(calendar_ != null) { "calendar must not be null" }
                    dateTimeArgs.add(calendar_)
                }
                NULL -> {}
                DIRECT -> sqlTypeName = SqlTypeName.ANY
                SHIFT -> when (sqlTypeName) {
                    TIMESTAMP, DATE -> offset = true
                    else -> {}
                }
                else -> {}
            }
            val source: Expression
            source = when (sqlTypeName) {
                DATE, TIME, TIMESTAMP -> Expressions.call(
                    getMethod(sqlTypeName, fieldType.isNullable(), offset),
                    Expressions.< Expression > list < Expression ? > ()
                        .append(
                            Expressions.call(
                                resultSet_,
                                getMethod2(sqlTypeName), dateTimeArgs
                            )
                        )
                        .appendIf(offset, getTimeZoneExpression(implementor))
                )
                ARRAY -> {
                    val x: Expression = Expressions.convert_(
                        Expressions.call(
                            resultSet_, jdbcGetMethod(primitive),
                            Expressions.constant(i + 1)
                        ),
                        java.sql.Array::class.java
                    )
                    Expressions.call(BuiltInMethod.JDBC_ARRAY_TO_LIST.method, x)
                }
                else -> Expressions.call(
                    resultSet_, jdbcGetMethod(primitive), Expressions.constant(i + 1)
                )
            }
            builder.add(
                Expressions.statement(
                    Expressions.assign(
                        target, source
                    )
                )
            )

            // [CALCITE-596] If primitive type columns contain null value, returns null
            // object
            if (primitive != null) {
                builder.add(
                    Expressions.ifThen(
                        Expressions.call(resultSet_, "wasNull"),
                        Expressions.statement(
                            Expressions.assign(
                                target,
                                Expressions.constant(null)
                            )
                        )
                    )
                )
            }
        }

        private fun getMethod(
            sqlTypeName: SqlTypeName, nullable: Boolean,
            offset: Boolean
        ): Method {
            return when (sqlTypeName) {
                DATE -> (if (nullable) BuiltInMethod.DATE_TO_INT_OPTIONAL else BuiltInMethod.DATE_TO_INT).method
                TIME -> (if (nullable) BuiltInMethod.TIME_TO_INT_OPTIONAL else BuiltInMethod.TIME_TO_INT).method
                TIMESTAMP -> (if (nullable) if (offset) BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL_OFFSET else BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL else if (offset) BuiltInMethod.TIMESTAMP_TO_LONG_OFFSET else BuiltInMethod.TIMESTAMP_TO_LONG).method
                else -> throw AssertionError(sqlTypeName.toString() + ":" + nullable)
            }
        }

        private fun getMethod2(sqlTypeName: SqlTypeName): Method {
            return when (sqlTypeName) {
                DATE -> BuiltInMethod.RESULT_SET_GET_DATE2.method
                TIME -> BuiltInMethod.RESULT_SET_GET_TIME2.method
                TIMESTAMP -> BuiltInMethod.RESULT_SET_GET_TIMESTAMP2.method
                else -> throw AssertionError(sqlTypeName)
            }
        }

        /** E,g, `jdbcGetMethod(int)` returns "getInt".  */
        private fun jdbcGetMethod(@Nullable primitive: Primitive?): String {
            return if (primitive == null) "getObject" else "get" + SqlFunctions.initcap(castNonNull(primitive.primitiveName))
        }
    }
}
