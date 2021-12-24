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
package org.apache.calcite.prepare

import org.apache.calcite.jdbc.CalciteSchema

/**
 * Implementation of [QueryableFactory]
 * that builds a tree of [RelNode] planner nodes. Used by
 * [LixToRelTranslator].
 *
 *
 * Each of the methods that implements a `Replayer` method creates
 * a tree of `RelNode`s equivalent to the arguments, and calls
 * [.setRel] to assign the root of that tree to the [.rel] member
 * variable.
 *
 *
 * To comply with the [org.apache.calcite.linq4j.QueryableFactory]
 * interface, which is after all a factory, each method returns a dummy result
 * such as `null` or `0`.
 * The caller will not use the result.
 * The real effect of the method is to
 * call [.setRel] with a `RelNode`.
 *
 *
 * NOTE: Many methods currently throw [UnsupportedOperationException].
 * These method need to be implemented.
 *
 * @param <T> Element type
</T> */
internal class QueryableRelBuilder<T>(translator: LixToRelTranslator) : QueryableFactory<T> {
    private val translator: LixToRelTranslator

    @Nullable
    private var rel: RelNode? = null

    init {
        this.translator = translator
    }

    fun toRel(queryable: Queryable<T>): RelNode {
        if (queryable is QueryableDefaults.Replayable) {
            (queryable as QueryableDefaults.Replayable).replay(this)
            return requireNonNull(rel, "rel")
        }
        if (queryable is AbstractTableQueryable) {
            val tableQueryable: AbstractTableQueryable = queryable as AbstractTableQueryable
            val table: QueryableTable = tableQueryable.table
            val tableEntry: CalciteSchema.TableEntry = CalciteSchema.from(tableQueryable.schema)
                .add(tableQueryable.tableName, tableQueryable.table)
            val relOptTable: RelOptTableImpl = RelOptTableImpl.create(
                null, table.getRowType(translator.typeFactory),
                tableEntry, null
            )
            return if (table is TranslatableTable) {
                (table as TranslatableTable).toRel(
                    translator.toRelContext(),
                    relOptTable
                )
            } else {
                LogicalTableScan.create(translator.cluster, relOptTable, ImmutableList.of())
            }
        }
        return translator.translate(
            requireNonNull(
                queryable.getExpression()
            ) { "null expression from $queryable" })
    }

    /** Sets the output of this event.  */
    private fun setRel(rel: RelNode) {
        this.rel = rel
    }

    // ~ Methods from QueryableFactory -----------------------------------------
    @Override
    fun <TAccumulate, TResult> aggregate(
        source: Queryable<T>?,
        seed: TAccumulate,
        func: FunctionExpression<Function2<TAccumulate, T, TAccumulate>?>?,
        selector: FunctionExpression<Function1<TAccumulate, TResult>?>?
    ): TResult {
        throw UnsupportedOperationException()
    }

    @Override
    fun aggregate(
        source: Queryable<T>?,
        selector: FunctionExpression<Function2<T, T, T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TAccumulate> aggregate(
        source: Queryable<T>?,
        seed: TAccumulate,
        selector: FunctionExpression<Function2<TAccumulate, T, TAccumulate>?>?
    ): TAccumulate {
        throw UnsupportedOperationException()
    }

    @Override
    fun all(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun any(source: Queryable<T>?): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun any(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageBigDecimal(
        source: Queryable<T>?,
        selector: FunctionExpression<BigDecimalFunction1<T>?>?
    ): BigDecimal {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageNullableBigDecimal(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableBigDecimalFunction1<T>?>?
    ): BigDecimal {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageDouble(
        source: Queryable<T>?,
        selector: FunctionExpression<DoubleFunction1<T>?>?
    ): Double {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageNullableDouble(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableDoubleFunction1<T>?>?
    ): Double {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageInteger(
        source: Queryable<T>?,
        selector: FunctionExpression<IntegerFunction1<T>?>?
    ): Int {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageNullableInteger(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableIntegerFunction1<T>?>?
    ): Integer {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageFloat(
        source: Queryable<T>?,
        selector: FunctionExpression<FloatFunction1<T>?>?
    ): Float {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageNullableFloat(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableFloatFunction1<T>?>?
    ): Float {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageLong(
        source: Queryable<T>?,
        selector: FunctionExpression<LongFunction1<T>?>?
    ): Long {
        throw UnsupportedOperationException()
    }

    @Override
    fun averageNullableLong(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableLongFunction1<T>?>?
    ): Long {
        throw UnsupportedOperationException()
    }

    @Override
    fun concat(
        source: Queryable<T>?, source2: Enumerable<T>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun contains(
        source: Queryable<T>?, element: T
    ): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun contains(
        source: Queryable<T>?, element: T, comparer: EqualityComparer<T>?
    ): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun count(source: Queryable<T>?): Int {
        throw UnsupportedOperationException()
    }

    @Override
    fun count(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): Int {
        throw UnsupportedOperationException()
    }

    @Override
    fun defaultIfEmpty(source: Queryable<T>?): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun defaultIfEmpty(
        source: Queryable<T>?,
        @PolyNull value: T
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun distinct(
        source: Queryable<T>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun distinct(
        source: Queryable<T>?, comparer: EqualityComparer<T>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun elementAt(source: Queryable<T>?, index: Int): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun elementAtOrDefault(source: Queryable<T>?, index: Int): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun except(
        source: Queryable<T>?, enumerable: Enumerable<T>?
    ): Queryable<T> {
        return except(source, enumerable, false)
    }

    @Override
    fun except(
        source: Queryable<T>?, enumerable: Enumerable<T>?, all: Boolean
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun except(
        source: Queryable<T>?,
        enumerable: Enumerable<T>?,
        tEqualityComparer: EqualityComparer<T>?
    ): Queryable<T> {
        return except(source, enumerable, tEqualityComparer, false)
    }

    @Override
    fun except(
        source: Queryable<T>?,
        enumerable: Enumerable<T>?,
        tEqualityComparer: EqualityComparer<T>?,
        all: Boolean
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun first(source: Queryable<T>?): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun first(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun firstOrDefault(
        source: Queryable<T>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun firstOrDefault(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey> groupBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?
    ): Queryable<Grouping<TKey, T>> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey> groupBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        comparer: EqualityComparer<TKey>?
    ): Queryable<Grouping<TKey, T>> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey, TElement> groupBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        elementSelector: FunctionExpression<Function1<T, TElement>?>?
    ): Queryable<Grouping<TKey, TElement>> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey, TResult> groupByK(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        resultSelector: FunctionExpression<Function2<TKey, Enumerable<T>?, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey, TElement> groupBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        elementSelector: FunctionExpression<Function1<T, TElement>?>?,
        comparer: EqualityComparer<TKey>?
    ): Queryable<Grouping<TKey, TElement>> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey, TResult> groupByK(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        elementSelector: FunctionExpression<Function2<TKey, Enumerable<T>?, TResult>?>?,
        comparer: EqualityComparer<TKey>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey, TElement, TResult> groupBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        elementSelector: FunctionExpression<Function1<T, TElement>?>?,
        resultSelector: FunctionExpression<Function2<TKey, Enumerable<TElement>?, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey, TElement, TResult> groupBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        elementSelector: FunctionExpression<Function1<T, TElement>?>?,
        resultSelector: FunctionExpression<Function2<TKey, Enumerable<TElement>?, TResult>?>?,
        comparer: EqualityComparer<TKey>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TInner, TKey, TResult> groupJoin(
        source: Queryable<T>?,
        inner: Enumerable<TInner>?,
        outerKeySelector: FunctionExpression<Function1<T, TKey>?>?,
        innerKeySelector: FunctionExpression<Function1<TInner, TKey>?>?,
        resultSelector: FunctionExpression<Function2<T, Enumerable<TInner>?, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TInner, TKey, TResult> groupJoin(
        source: Queryable<T>?,
        inner: Enumerable<TInner>?,
        outerKeySelector: FunctionExpression<Function1<T, TKey>?>?,
        innerKeySelector: FunctionExpression<Function1<TInner, TKey>?>?,
        resultSelector: FunctionExpression<Function2<T, Enumerable<TInner>?, TResult>?>?,
        comparer: EqualityComparer<TKey>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun intersect(
        source: Queryable<T>?, enumerable: Enumerable<T>?
    ): Queryable<T> {
        return intersect(source, enumerable, false)
    }

    @Override
    fun intersect(
        source: Queryable<T>?, enumerable: Enumerable<T>?, all: Boolean
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun intersect(
        source: Queryable<T>?,
        enumerable: Enumerable<T>?,
        tEqualityComparer: EqualityComparer<T>?
    ): Queryable<T> {
        return intersect(source, enumerable, tEqualityComparer, false)
    }

    @Override
    fun intersect(
        source: Queryable<T>?,
        enumerable: Enumerable<T>?,
        tEqualityComparer: EqualityComparer<T>?, all: Boolean
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TInner, TKey, TResult> join(
        source: Queryable<T>?,
        inner: Enumerable<TInner>?,
        outerKeySelector: FunctionExpression<Function1<T, TKey>?>?,
        innerKeySelector: FunctionExpression<Function1<TInner, TKey>?>?,
        resultSelector: FunctionExpression<Function2<T, TInner, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TInner, TKey, TResult> join(
        source: Queryable<T>?,
        inner: Enumerable<TInner>?,
        outerKeySelector: FunctionExpression<Function1<T, TKey>?>?,
        innerKeySelector: FunctionExpression<Function1<TInner, TKey>?>?,
        resultSelector: FunctionExpression<Function2<T, TInner, TResult>?>?,
        comparer: EqualityComparer<TKey>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun last(source: Queryable<T>?): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun last(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun lastOrDefault(
        source: Queryable<T>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun lastOrDefault(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun longCount(source: Queryable<T>?): Long {
        throw UnsupportedOperationException()
    }

    @Override
    fun longCount(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): Long {
        throw UnsupportedOperationException()
    }

    @Override
    fun max(source: Queryable<T>?): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TResult : Comparable<TResult>?> max(
        source: Queryable<T>?,
        selector: FunctionExpression<Function1<T, TResult>?>?
    ): TResult {
        throw UnsupportedOperationException()
    }

    @Override
    fun min(source: Queryable<T>?): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TResult : Comparable<TResult>?> min(
        source: Queryable<T>?,
        selector: FunctionExpression<Function1<T, TResult>?>?
    ): TResult {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TResult> ofType(
        source: Queryable<T>?, clazz: Class<TResult>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <T2> cast(
        source: Queryable<T>?,
        clazz: Class<T2>?
    ): Queryable<T2> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey : Comparable?> orderBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey> orderBy(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        comparator: Comparator<TKey>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey : Comparable?> orderByDescending(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey> orderByDescending(
        source: Queryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        comparator: Comparator<TKey>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun reverse(
        source: Queryable<T>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TResult> select(
        source: Queryable<T>,
        selector: FunctionExpression<Function1<T, TResult>?>
    ): Queryable<TResult> {
        val child: RelNode = toRel(source)
        val nodes: List<RexNode> = translator.toRexList(selector, child)
        setRel(
            LogicalProject.create(child, ImmutableList.of(), nodes, null as List<String?>?)
        )
        return castNonNull(null)
    }

    @Override
    fun <TResult> selectN(
        source: Queryable<T>?,
        selector: FunctionExpression<Function2<T, Integer?, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TResult> selectMany(
        source: Queryable<T>?,
        selector: FunctionExpression<Function1<T, Enumerable<TResult>?>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TResult> selectManyN(
        source: Queryable<T>?,
        selector: FunctionExpression<Function2<T, Integer?, Enumerable<TResult>?>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TCollection, TResult> selectMany(
        source: Queryable<T>?,
        collectionSelector: FunctionExpression<Function2<T, Integer?, Enumerable<TCollection>?>?>?,
        resultSelector: FunctionExpression<Function2<T, TCollection, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TCollection, TResult> selectManyN(
        source: Queryable<T>?,
        collectionSelector: FunctionExpression<Function1<T, Enumerable<TCollection>?>?>?,
        resultSelector: FunctionExpression<Function2<T, TCollection, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }

    @Override
    fun sequenceEqual(
        source: Queryable<T>?, enumerable: Enumerable<T>?
    ): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun sequenceEqual(
        source: Queryable<T>?,
        enumerable: Enumerable<T>?,
        tEqualityComparer: EqualityComparer<T>?
    ): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun single(source: Queryable<T>?): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun single(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun singleOrDefault(source: Queryable<T>?): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun singleOrDefault(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): T {
        throw UnsupportedOperationException()
    }

    @Override
    fun skip(
        source: Queryable<T>?, count: Int
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun skipWhile(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun skipWhileN(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate2<T, Integer?>?>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumBigDecimal(
        source: Queryable<T>?,
        selector: FunctionExpression<BigDecimalFunction1<T>?>?
    ): BigDecimal {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumNullableBigDecimal(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableBigDecimalFunction1<T>?>?
    ): BigDecimal {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumDouble(
        source: Queryable<T>?,
        selector: FunctionExpression<DoubleFunction1<T>?>?
    ): Double {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumNullableDouble(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableDoubleFunction1<T>?>?
    ): Double {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumInteger(
        source: Queryable<T>?,
        selector: FunctionExpression<IntegerFunction1<T>?>?
    ): Int {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumNullableInteger(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableIntegerFunction1<T>?>?
    ): Integer {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumLong(
        source: Queryable<T>?,
        selector: FunctionExpression<LongFunction1<T>?>?
    ): Long {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumNullableLong(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableLongFunction1<T>?>?
    ): Long {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumFloat(
        source: Queryable<T>?,
        selector: FunctionExpression<FloatFunction1<T>?>?
    ): Float {
        throw UnsupportedOperationException()
    }

    @Override
    fun sumNullableFloat(
        source: Queryable<T>?,
        selector: FunctionExpression<NullableFloatFunction1<T>?>?
    ): Float {
        throw UnsupportedOperationException()
    }

    @Override
    fun take(
        source: Queryable<T>?, count: Int
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun takeWhile(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate1<T>?>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun takeWhileN(
        source: Queryable<T>?,
        predicate: FunctionExpression<Predicate2<T, Integer?>?>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey : Comparable<TKey>?> thenBy(
        source: OrderedQueryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey> thenBy(
        source: OrderedQueryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        comparator: Comparator<TKey>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey : Comparable<TKey>?> thenByDescending(
        source: OrderedQueryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <TKey> thenByDescending(
        source: OrderedQueryable<T>?,
        keySelector: FunctionExpression<Function1<T, TKey>?>?,
        comparator: Comparator<TKey>?
    ): OrderedQueryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun union(
        source: Queryable<T>?, source1: Enumerable<T>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun union(
        source: Queryable<T>?,
        source1: Enumerable<T>?,
        tEqualityComparer: EqualityComparer<T>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun where(
        source: Queryable<T>,
        predicate: FunctionExpression<out Predicate1<T>?>
    ): Queryable<T> {
        val child: RelNode = toRel(source)
        val node: RexNode = translator.toRex(predicate, child)
        setRel(LogicalFilter.create(child, node))
        return source
    }

    @Override
    fun whereN(
        source: Queryable<T>?,
        predicate: FunctionExpression<out Predicate2<T, Integer?>?>?
    ): Queryable<T> {
        throw UnsupportedOperationException()
    }

    @Override
    fun <T1, TResult> zip(
        source: Queryable<T>?,
        source1: Enumerable<T1>?,
        resultSelector: FunctionExpression<Function2<T, T1, TResult>?>?
    ): Queryable<TResult> {
        throw UnsupportedOperationException()
    }
}
