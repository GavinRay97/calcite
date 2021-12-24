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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.rel.type.RelDataType

/**
 * Definition of the `MIN` and `MAX` aggregate functions,
 * returning the returns the smallest/largest of the values which go into it.
 *
 *
 * There are 3 forms:
 *
 * <dl>
 * <dt>min/max(*primitive type*)
</dt> * <dd>values are compared using '&lt;'
 *
</dd> * <dt>min/max([java.lang.Comparable])
</dt> * <dd>values are compared using [java.lang.Comparable.compareTo]
 *
</dd> * <dt>min/max([java.util.Comparator], [java.lang.Object])
</dt> * <dd>the [java.util.Comparator.compare] method of the comparator is used
 * to compare pairs of objects. The comparator is a startup argument, and must
 * therefore be constant for the duration of the aggregation.
</dd></dl> *
 */
class SqlMinMaxAggFunction(
    funcName: String?, kind: SqlKind,
    inputTypeChecker: SqlOperandTypeChecker?
) : SqlAggFunction(
    funcName,
    null,
    kind,
    ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
    null,
    inputTypeChecker,
    SqlFunctionCategory.SYSTEM,
    false,
    false,
    Optionality.FORBIDDEN
) {
    //~ Instance fields --------------------------------------------------------
    @Deprecated // to be removed before 2.0
    val argTypes: List<RelDataType>

    // to be removed before 2.0
    @get:Deprecated
    val minMaxKind: Int
    //~ Constructors -----------------------------------------------------------
    /** Creates a SqlMinMaxAggFunction.  */
    constructor(kind: SqlKind) : this(kind.name(), kind, OperandTypes.COMPARABLE_ORDERED) {}

    /** Creates a SqlMinMaxAggFunction.  */
    init {
        argTypes = ImmutableList.of()
        minMaxKind = MINMAX_COMPARABLE
        Preconditions.checkArgument(
            kind === SqlKind.MIN
                    || kind === SqlKind.MAX
        )
    }

    @Deprecated // to be removed before 2.0
    constructor(
        argTypes: List<RelDataType?>,
        isMin: Boolean,
        minMaxKind: Int
    ) : this(if (isMin) SqlKind.MIN else SqlKind.MAX) {
        assert(argTypes.isEmpty())
        assert(minMaxKind == MINMAX_COMPARABLE)
    }

    //~ Methods ----------------------------------------------------------------
    // to be removed before 2.0
    @get:Deprecated
    val isMin: Boolean
        get() = kind === SqlKind.MIN

    @get:Override
    val distinctOptionality: Optionality
        get() = Optionality.IGNORED

    @SuppressWarnings("deprecation")
    @Override
    fun getParameterTypes(typeFactory: RelDataTypeFactory?): List<RelDataType> {
        return when (minMaxKind) {
            MINMAX_PRIMITIVE, MINMAX_COMPARABLE -> argTypes
            MINMAX_COMPARATOR -> argTypes.subList(1, 2)
            else -> throw AssertionError("bad kind: $minMaxKind")
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    fun getReturnType(typeFactory: RelDataTypeFactory?): RelDataType {
        return when (minMaxKind) {
            MINMAX_PRIMITIVE, MINMAX_COMPARABLE -> argTypes[0]
            MINMAX_COMPARATOR -> argTypes[1]
            else -> throw AssertionError("bad kind: $minMaxKind")
        }
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        return if (clazz === SqlSplittableAggFunction::class.java) {
            clazz.cast(SqlSplittableAggFunction.SelfSplitter.INSTANCE)
        } else super.unwrap(clazz)
    }

    @get:Override
    val rollup: SqlAggFunction
        get() = this

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        const val MINMAX_INVALID = -1
        const val MINMAX_PRIMITIVE = 0
        const val MINMAX_COMPARABLE = 1
        const val MINMAX_COMPARATOR = 2
    }
}
