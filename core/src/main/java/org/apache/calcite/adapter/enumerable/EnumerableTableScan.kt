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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Implementation of [org.apache.calcite.rel.core.TableScan] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableTableScan(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    table: RelOptTable, elementType: Class
) : TableScan(cluster, traitSet, ImmutableList.of(), table), EnumerableRel {
    private val elementType: Class

    /** Creates an EnumerableTableScan.
     *
     *
     * Use [.create] unless you know what you are doing.  */
    init {
        assert(getConvention() is EnumerableConvention)
        this.elementType = elementType
        assert(canHandle(table)) { "EnumerableTableScan can't implement $table, see EnumerableTableScan#canHandle" }
    }

    /**
     * Code snippet to demonstrate how to generate IndexScan on demand
     * by passing required collation through TableScan.
     *
     * @return IndexScan if there is index available on collation keys
     */
    @Override
    @Nullable
    fun passThrough(required: RelTraitSet?): RelNode? {
/*
    keys = required.getCollation().getKeys();
    if (table has index on keys) {
      direction = forward or backward;
      return new IndexScan(table, indexInfo, direction);
    }
*/
        return null
    }

    @get:Override
    val deriveMode: DeriveMode
        get() = DeriveMode.PROHIBITED

    private fun getExpression(physType: PhysType): Expression {
        val expression: Expression = table.getExpression(Queryable::class.java)
            ?: throw IllegalStateException(
                "Unable to implement " + RelOptUtil.toString(this, SqlExplainLevel.ALL_ATTRIBUTES)
                    .toString() + ": " + table.toString() + ".getExpression(Queryable.class) returned null"
            )
        val expression2: Expression = toEnumerable(expression)
        assert(Types.isAssignableFrom(Enumerable::class.java, expression2.getType()))
        return toRows(physType, expression2)
    }

    private fun toRows(physType: PhysType, expression: Expression): Expression {
        if (physType.getFormat() === JavaRowFormat.SCALAR && Array<Object>::class.java.isAssignableFrom(elementType)
            && getRowType().getFieldCount() === 1 && (table.unwrap(ScannableTable::class.java) != null || table.unwrap(
                FilterableTable::class.java
            ) != null || table.unwrap(ProjectableFilterableTable::class.java) != null)
        ) {
            return Expressions.call(BuiltInMethod.SLICE0.method, expression)
        }
        val oldFormat: JavaRowFormat = format()
        if (physType.getFormat() === oldFormat && !hasCollectionField(getRowType())) {
            return expression
        }
        val row_: ParameterExpression = Expressions.parameter(elementType, "row")
        val fieldCount: Int = table.getRowType().getFieldCount()
        val expressionList: List<Expression> = ArrayList(fieldCount)
        for (i in 0 until fieldCount) {
            expressionList.add(fieldExpression(row_, i, physType, oldFormat))
        }
        return Expressions.call(
            expression,
            BuiltInMethod.SELECT.method,
            Expressions.lambda(
                Function1::class.java, physType.record(expressionList),
                row_
            )
        )
    }

    private fun fieldExpression(
        row_: ParameterExpression, i: Int,
        physType: PhysType, format: JavaRowFormat
    ): Expression {
        val e: Expression = format.field(row_, i, null, physType.getJavaFieldType(i))
        val relFieldType: RelDataType = physType.getRowType().getFieldList().get(i).getType()
        return when (relFieldType.getSqlTypeName()) {
            ARRAY, MULTISET -> {
                val fieldType: RelDataType = requireNonNull(
                    relFieldType.getComponentType()
                ) { "relFieldType.getComponentType() for $relFieldType" }
                if (fieldType.isStruct()) {
                    // We can't represent a multiset or array as a List<Employee>, because
                    // the consumer does not know the element type.
                    // The standard element type is List.
                    // We need to convert to a List<List>.
                    val typeFactory: JavaTypeFactory = getCluster().getTypeFactory() as JavaTypeFactory
                    val elementPhysType: PhysType = PhysTypeImpl.of(
                        typeFactory, fieldType, JavaRowFormat.CUSTOM
                    )
                    val e2: MethodCallExpression = Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, e)
                    val e3: Expression = elementPhysType.convertTo(e2, JavaRowFormat.LIST)
                    Expressions.call(e3, BuiltInMethod.ENUMERABLE_TO_LIST.method)
                } else {
                    e
                }
            }
            else -> e
        }
    }

    private fun format(): JavaRowFormat {
        val fieldCount: Int = getRowType().getFieldCount()
        if (fieldCount == 0) {
            return JavaRowFormat.LIST
        }
        if (Array<Object>::class.java.isAssignableFrom(elementType)) {
            return if (fieldCount == 1) JavaRowFormat.SCALAR else JavaRowFormat.ARRAY
        }
        if (Row::class.java.isAssignableFrom(elementType)) {
            return JavaRowFormat.ROW
        }
        return if (fieldCount == 1 && (Object::class.java === elementType || Primitive.`is`(elementType)
                    || Number::class.java.isAssignableFrom(elementType)
                    || String::class.java === elementType)
        ) {
            JavaRowFormat.SCALAR
        } else JavaRowFormat.CUSTOM
    }

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return EnumerableTableScan(getCluster(), traitSet, table, elementType)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer?): Result {
        // Note that representation is ARRAY. This assumes that the table
        // returns a Object[] for each record. Actually a Table<T> can
        // return any type T. And, if it is a JdbcTable, we'd like to be
        // able to generate alternate accessors that return e.g. synthetic
        // records {T0 f0; T1 f1; ...} and don't box every primitive value.
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            format()
        )
        val expression: Expression = getExpression(physType)
        return implementor.result(physType, Blocks.toBlock(expression))
    }

    companion object {
        /** Creates an EnumerableTableScan.  */
        fun create(
            cluster: RelOptCluster,
            relOptTable: RelOptTable
        ): EnumerableTableScan {
            val table: Table = relOptTable.unwrap(Table::class.java)
            val elementType: Class = deduceElementType(table)
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(RelCollationTraitDef.INSTANCE) {
                    if (table != null) {
                        return@replaceIfs table.getStatistic().getCollations()
                    }
                    ImmutableList.of()
                }
            return EnumerableTableScan(cluster, traitSet, relOptTable, elementType)
        }

        /** Returns whether EnumerableTableScan can generate code to handle a
         * particular variant of the Table SPI.
         */
        @Deprecated
        @Deprecated("remove before Calcite 2.0")
        fun canHandle(table: Table?): Boolean {
            return if (table is TransientTable) {
                // CALCITE-3673: TransientTable can't be implemented with Enumerable
                false
            } else table is QueryableTable
                    || table is FilterableTable
                    || table is ProjectableFilterableTable
                    || table is ScannableTable
            // See org.apache.calcite.prepare.RelOptTableImpl.getClassExpressionFunction
        }

        /** Returns whether EnumerableTableScan can generate code to handle a
         * particular variant of the Table SPI.
         */
        fun canHandle(relOptTable: RelOptTable): Boolean {
            val table: Table = relOptTable.unwrap(Table::class.java)
            if (table != null && !canHandle(table)) {
                return false
            }
            val supportArray: Boolean = CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_ARRAY.value()
            val supportMap: Boolean = CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_MAP.value()
            val supportMultiset: Boolean = CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_MULTISET.value()
            if (supportArray && supportMap && supportMultiset) {
                return true
            }
            // Struct fields are not supported in EnumerableTableScan
            for (field in relOptTable.getRowType().getFieldList()) {
                var unsupportedType = false
                when (field.getType().getSqlTypeName()) {
                    ARRAY -> unsupportedType = supportArray
                    MAP -> unsupportedType = supportMap
                    MULTISET -> unsupportedType = supportMultiset
                    else -> {}
                }
                if (unsupportedType) {
                    return false
                }
            }
            return true
        }

        fun deduceElementType(@Nullable table: Table?): Class {
            return if (table is QueryableTable) {
                val queryableTable: QueryableTable? = table as QueryableTable?
                val type: Type = queryableTable.getElementType()
                if (type is Class) {
                    type as Class
                } else {
                    Array<Object>::class.java
                }
            } else if (table is ScannableTable
                || table is FilterableTable
                || table is ProjectableFilterableTable
                || table is StreamableTable
            ) {
                Array<Object>::class.java
            } else {
                Object::class.java
            }
        }

        fun deduceFormat(table: RelOptTable): JavaRowFormat {
            val elementType: Class = deduceElementType(
                table.unwrapOrThrow(
                    Table::class.java
                )
            )
            return if (elementType === Array<Object>::class.java) JavaRowFormat.ARRAY else JavaRowFormat.CUSTOM
        }

        private fun toEnumerable(expression: Expression): Expression {
            var expression: Expression = expression
            val type: Type = expression.getType()
            if (Types.isArray(type)) {
                if (requireNonNull(toClass(type).getComponentType()).isPrimitive()) {
                    expression = Expressions.call(BuiltInMethod.AS_LIST.method, expression)
                }
                return Expressions.call(BuiltInMethod.AS_ENUMERABLE.method, expression)
            } else if (Types.isAssignableFrom(Iterable::class.java, type)
                && !Types.isAssignableFrom(Enumerable::class.java, type)
            ) {
                return Expressions.call(
                    BuiltInMethod.AS_ENUMERABLE2.method,
                    expression
                )
            } else if (Types.isAssignableFrom(Queryable::class.java, type)) {
                // Queryable extends Enumerable, but it's too "clever", so we call
                // Queryable.asEnumerable so that operations such as take(int) will be
                // evaluated directly.
                return Expressions.call(
                    expression,
                    BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method
                )
            }
            return expression
        }

        private fun hasCollectionField(rowType: RelDataType): Boolean {
            for (field in rowType.getFieldList()) {
                when (field.getType().getSqlTypeName()) {
                    ARRAY, MULTISET -> return true
                    else -> {}
                }
            }
            return false
        }
    }
}
