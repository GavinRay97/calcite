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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.Ord

/**
 * Call to an aggregate function within an
 * [org.apache.calcite.rel.core.Aggregate].
 */
class AggregateCall private constructor(
    aggFunction: SqlAggFunction, distinct: Boolean,
    approximate: Boolean, ignoreNulls: Boolean, argList: List<Integer>,
    filterArg: Int, @Nullable distinctKeys: ImmutableBitSet?,
    collation: RelCollation, type: RelDataType?, @Nullable name: String
) {
    //~ Instance fields --------------------------------------------------------
    private val aggFunction: SqlAggFunction

    /**
     * Returns whether this AggregateCall is distinct, as in `
     * COUNT(DISTINCT empno)`.
     *
     * @return whether distinct
     */
    val isDistinct: Boolean

    /**
     * Returns whether this AggregateCall is approximate, as in `
     * APPROX_COUNT_DISTINCT(empno)`.
     *
     * @return whether approximate
     */
    val isApproximate: Boolean
    private val ignoreNulls: Boolean
    val type: RelDataType

    /**
     * Returns the name.
     *
     * @return name
     */
    @get:Nullable
    @Nullable
    val name: String

    // We considered using ImmutableIntList but we would not save much memory:
    // since all values are small, ImmutableList uses cached Integer values.
    private val argList: ImmutableList<Integer>
    val filterArg: Int

    @Nullable
    val distinctKeys: ImmutableBitSet?
    val collation: RelCollation
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AggregateCall.
     *
     * @param aggFunction Aggregate function
     * @param distinct    Whether distinct
     * @param argList     List of ordinals of arguments
     * @param type        Result type
     * @param name        Name (may be null)
     */
    @Deprecated // to be removed before 2.0
    constructor(
        aggFunction: SqlAggFunction,
        distinct: Boolean,
        argList: List<Integer>,
        type: RelDataType?,
        name: String
    ) : this(
        aggFunction, distinct, false, false,
        argList, -1, null, RelCollations.EMPTY, type, name
    ) {
    }

    /**
     * Creates an AggregateCall.
     *
     * @param aggFunction Aggregate function
     * @param distinct    Whether distinct
     * @param approximate Whether approximate
     * @param argList     List of ordinals of arguments
     * @param filterArg   Ordinal of filter argument (the
     * `FILTER (WHERE ...)` clause in SQL), or -1
     * @param distinctKeys Ordinals of fields to make values distinct on before
     * aggregating, or null
     * @param collation   How to sort values before aggregation (the
     * `WITHIN GROUP` clause in SQL)
     * @param type        Result type
     * @param name        Name (may be null)
     */
    init {
        this.type = Objects.requireNonNull(type, "type")
        this.name = name
        this.aggFunction = Objects.requireNonNull(aggFunction, "aggFunction")
        this.argList = ImmutableList.copyOf(argList)
        this.distinctKeys = distinctKeys
        this.filterArg = filterArg
        this.collation = Objects.requireNonNull(collation, "collation")
        isDistinct = distinct
        isApproximate = approximate
        this.ignoreNulls = ignoreNulls
        Preconditions.checkArgument(
            aggFunction.getDistinctOptionality() !== Optionality.IGNORED || !distinct,
            "DISTINCT has no effect for this aggregate function, so must be false"
        )
        Preconditions.checkArgument(filterArg < 0 || aggFunction.allowsFilter())
    }

    /** Withs [.isDistinct].  */
    fun withDistinct(distinct: Boolean): AggregateCall {
        return if (distinct == isDistinct) this else AggregateCall(
            aggFunction, distinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    /** Withs [.isApproximate].  */
    fun withApproximate(approximate: Boolean): AggregateCall {
        return if (approximate == isApproximate) this else AggregateCall(
            aggFunction, isDistinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    /**
     * Returns whether this AggregateCall ignores nulls.
     *
     * @return whether ignore nulls
     */
    fun ignoreNulls(): Boolean {
        return ignoreNulls
    }

    /** Withs [.ignoreNulls].  */
    fun withIgnoreNulls(ignoreNulls: Boolean): AggregateCall {
        return if (ignoreNulls == this.ignoreNulls) this else AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    /**
     * Returns the aggregate function.
     *
     * @return aggregate function
     */
    val aggregation: SqlAggFunction
        get() = aggFunction

    /**
     * Returns the aggregate ordering definition (the `WITHIN GROUP` clause
     * in SQL), or the empty list if not specified.
     *
     * @return ordering definition
     */
    fun getCollation(): RelCollation {
        return collation
    }

    /** Withs [.getCollation].  */
    fun withCollation(collation: RelCollation): AggregateCall {
        return if (collation.equals(this.collation)) this else AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    /**
     * Returns the ordinals of the arguments to this call.
     *
     *
     * The list is immutable.
     *
     * @return list of argument ordinals
     */
    fun getArgList(): List<Integer> {
        return argList
    }

    /** Withs [.getArgList].  */
    fun withArgList(argList: List<Integer>): AggregateCall {
        return if (argList.equals(this.argList)) this else AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    /** Withs [.distinctKeys].  */
    fun withDistinctKeys(
        @Nullable distinctKeys: ImmutableBitSet?
    ): AggregateCall {
        return if (Objects.equals(distinctKeys, this.distinctKeys)) this else AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    /**
     * Returns the result type.
     *
     * @return result type
     */
    fun getType(): RelDataType {
        return type
    }

    /** Withs [.name].  */
    fun withName(@Nullable name: String): AggregateCall {
        return if (Objects.equals(name, this.name)) this else AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    @Deprecated // to be removed before 2.0
    fun rename(@Nullable name: String): AggregateCall {
        return withName(name)
    }

    @Override
    override fun toString(): String {
        val buf = StringBuilder(aggFunction.toString())
        buf.append("(")
        if (isApproximate) {
            buf.append("APPROXIMATE ")
        }
        if (isDistinct) {
            buf.append(if (argList.size() === 0) "DISTINCT" else "DISTINCT ")
        }
        var i = -1
        for (arg in argList) {
            if (++i > 0) {
                buf.append(", ")
            }
            buf.append("$")
            buf.append(arg)
        }
        buf.append(")")
        if (distinctKeys != null) {
            buf.append(" WITHIN DISTINCT (")
            for (key in Ord.zip(distinctKeys)) {
                buf.append(if (key.i > 0) ", $" else "$")
                buf.append(key.e)
            }
            buf.append(")")
        }
        if (!collation.equals(RelCollations.EMPTY)) {
            buf.append(" WITHIN GROUP (")
            buf.append(collation)
            buf.append(")")
        }
        if (hasFilter()) {
            buf.append(" FILTER $")
            buf.append(filterArg)
        }
        return buf.toString()
    }

    /** Returns whether this AggregateCall has a filter argument.  */
    fun hasFilter(): Boolean {
        return filterArg >= 0
    }

    /** Withs [.filterArg].  */
    fun withFilter(filterArg: Int): AggregateCall {
        return if (filterArg == this.filterArg) this else AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name
        )
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return (o === this
                || (o is AggregateCall
                && aggFunction.equals((o as AggregateCall).aggFunction)
                && isDistinct == (o as AggregateCall).isDistinct && isApproximate == (o as AggregateCall).isApproximate && ignoreNulls == (o as AggregateCall).ignoreNulls && argList.equals(
            (o as AggregateCall).argList
        )
                && filterArg == (o as AggregateCall).filterArg && Objects.equals(
            distinctKeys,
            (o as AggregateCall).distinctKeys
        )
                && collation.equals((o as AggregateCall).collation)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation
        )
    }

    /**
     * Creates a binding of this call in the context of an
     * [org.apache.calcite.rel.logical.LogicalAggregate],
     * which can then be used to infer the return type.
     */
    fun createBinding(
        aggregateRelBase: Aggregate
    ): Aggregate.AggCallBinding {
        val rowType: RelDataType = aggregateRelBase.getInput().getRowType()
        return AggCallBinding(
            aggregateRelBase.getCluster().getTypeFactory(), aggFunction,
            SqlTypeUtil.projectTypes(rowType, argList),
            aggregateRelBase.getGroupCount(), hasFilter()
        )
    }

    /**
     * Creates an equivalent AggregateCall with new argument ordinals.
     *
     * @see .transform
     * @param args Arguments
     * @return AggregateCall that suits new inputs and GROUP BY columns
     */
    @Deprecated // to be removed before 2.0
    fun copy(
        args: List<Integer>, filterArg: Int,
        @Nullable distinctKeys: ImmutableBitSet?, collation: RelCollation
    ): AggregateCall {
        return AggregateCall(
            aggFunction, isDistinct, isApproximate, ignoreNulls,
            args, filterArg, distinctKeys, collation, type, name
        )
    }

    @Deprecated // to be removed before 2.0
    fun copy(
        args: List<Integer>, filterArg: Int,
        collation: RelCollation
    ): AggregateCall {
        // ignoring distinctKeys is error-prone
        return copy(args, filterArg, distinctKeys, collation)
    }

    @Deprecated // to be removed before 2.0
    fun copy(args: List<Integer>, filterArg: Int): AggregateCall {
        // ignoring distinctKeys, collation is error-prone
        return copy(args, filterArg, distinctKeys, collation)
    }

    @Deprecated // to be removed before 2.0
    fun copy(args: List<Integer>): AggregateCall {
        // ignoring filterArg, distinctKeys, collation is error-prone
        return copy(args, filterArg, distinctKeys, collation)
    }

    /**
     * Creates an equivalent AggregateCall that is adapted to a new input types
     * and/or number of columns in GROUP BY.
     *
     * @param input            Relation that will be input of Aggregate
     * @param argList          Argument indices of the new call in the input
     * @param filterArg        Index of the filter, or -1
     * @param oldGroupKeyCount number of columns in GROUP BY of old aggregate
     * @param newGroupKeyCount number of columns in GROUP BY of new aggregate
     * @return AggregateCall that suits new inputs and GROUP BY columns
     */
    fun adaptTo(
        input: RelNode, argList: List<Integer>,
        filterArg: Int, oldGroupKeyCount: Int, newGroupKeyCount: Int
    ): AggregateCall {
        // The return type of aggregate call need to be recomputed.
        // Since it might depend on the number of columns in GROUP BY.
        val newType: RelDataType? = if (oldGroupKeyCount == newGroupKeyCount && argList.equals(this.argList)
            && filterArg == this.filterArg
        ) type else null
        return create(
            aggFunction, isDistinct, isApproximate, ignoreNulls, argList,
            filterArg, distinctKeys, collation,
            newGroupKeyCount, input, newType, name
        )
    }

    /** Creates a copy of this aggregate call, applying a mapping to its
     * arguments.  */
    fun transform(mapping: Mappings.TargetMapping?): AggregateCall {
        return copy(
            Mappings.apply2(mapping as Mapping?, argList),
            if (hasFilter()) Mappings.apply(mapping, filterArg) else -1,
            if (distinctKeys == null) null else distinctKeys.permute(mapping),
            RelCollations.permute(collation, mapping)
        )
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, argList: List<Integer>, groupCount: Int, input: RelNode,
            @Nullable type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, false, false, argList, -1,
                null, RelCollations.EMPTY, groupCount, input, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, argList: List<Integer>, filterArg: Int, groupCount: Int,
            input: RelNode, @Nullable type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, false, false, argList, filterArg,
                null, RelCollations.EMPTY, groupCount, input, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, argList: List<Integer>,
            filterArg: Int, groupCount: Int,
            input: RelNode, @Nullable type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, approximate, false, argList,
                filterArg, null, RelCollations.EMPTY, groupCount, input, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, argList: List<Integer>,
            filterArg: Int, collation: RelCollation, groupCount: Int,
            input: RelNode, @Nullable type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, approximate, false, argList, filterArg,
                null, collation, groupCount, input, type, name
            )
        }

        /** Creates an AggregateCall, inferring its type if `type` is null.  */
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, ignoreNulls: Boolean,
            argList: List<Integer>, filterArg: Int,
            @Nullable distinctKeys: ImmutableBitSet?, collation: RelCollation,
            groupCount: Int,
            input: RelNode, @Nullable type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            var type: RelDataType? = type
            if (type == null) {
                val typeFactory: RelDataTypeFactory = input.getCluster().getTypeFactory()
                val types: List<RelDataType> = SqlTypeUtil.projectTypes(input.getRowType(), argList)
                val callBinding: Aggregate.AggCallBinding = AggCallBinding(
                    typeFactory, aggFunction, types,
                    groupCount, filterArg >= 0
                )
                type = aggFunction.inferReturnType(callBinding)
            }
            return create(
                aggFunction, distinct, approximate, ignoreNulls, argList,
                filterArg, distinctKeys, collation, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, argList: List<Integer>, filterArg: Int, type: RelDataType?,
            @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, false, false, argList, filterArg, null,
                RelCollations.EMPTY, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, argList: List<Integer>,
            filterArg: Int, type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, approximate, false, argList, filterArg,
                null, RelCollations.EMPTY, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, argList: List<Integer>,
            filterArg: Int, collation: RelCollation, type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, approximate, false, argList, filterArg,
                null, collation, type, name
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, ignoreNulls: Boolean,
            argList: List<Integer>, filterArg: Int, collation: RelCollation,
            type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            return create(
                aggFunction, distinct, approximate, ignoreNulls, argList,
                filterArg, null, collation, type, name
            )
        }

        /** Creates an AggregateCall.  */
        fun create(
            aggFunction: SqlAggFunction,
            distinct: Boolean, approximate: Boolean, ignoreNulls: Boolean,
            argList: List<Integer>, filterArg: Int,
            @Nullable distinctKeys: ImmutableBitSet?, collation: RelCollation,
            type: RelDataType?, @Nullable name: String
        ): AggregateCall {
            val distinct2 = (distinct
                    && aggFunction.getDistinctOptionality() !== Optionality.IGNORED)
            return AggregateCall(
                aggFunction, distinct2, approximate, ignoreNulls,
                argList, filterArg, distinctKeys, collation, type, name
            )
        }
    }
}
