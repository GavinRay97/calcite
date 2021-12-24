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

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.DateString
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.RangeSets
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.TimeString
import org.apache.calcite.util.TimestampString
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableMap
import com.google.common.collect.RangeSet
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Comparator
import java.util.EnumSet
import java.util.List
import java.util.Map
import java.util.function.IntPredicate

/**
 * Evaluates [RexNode] expressions.
 *
 *
 * Caveats:
 *
 *  * It uses interpretation, so it is not very efficient.
 *  * It is intended for testing, so does not cover very many functions and
 * operators. (Feel free to contribute more!)
 *  * It is not well tested.
 *
 */
class RexInterpreter private constructor(environment: Map<RexNode, Comparable>) : RexVisitor<Comparable?> {
    private val environment: Map<RexNode, Comparable>

    /** Creates an interpreter.
     *
     * @param environment Values of certain expressions (usually
     * [RexInputRef]s)
     */
    init {
        this.environment = ImmutableMap.copyOf(environment)
    }

    private fun getOrUnbound(e: RexNode): Comparable {
        val comparable: Comparable? = environment[e]
        if (comparable != null) {
            return comparable
        }
        throw unbound(e)
    }

    @Override
    fun visitInputRef(inputRef: RexInputRef): Comparable {
        return getOrUnbound(inputRef)
    }

    @Override
    fun visitLocalRef(localRef: RexLocalRef): Comparable {
        throw unbound(localRef)
    }

    @Override
    fun visitLiteral(literal: RexLiteral): Comparable {
        return Util.first(literal.getValue4(), N)
    }

    @Override
    fun visitOver(over: RexOver): Comparable {
        throw unbound(over)
    }

    @Override
    fun visitCorrelVariable(correlVariable: RexCorrelVariable): Comparable {
        return getOrUnbound(correlVariable)
    }

    @Override
    fun visitDynamicParam(dynamicParam: RexDynamicParam): Comparable {
        return getOrUnbound(dynamicParam)
    }

    @Override
    fun visitRangeRef(rangeRef: RexRangeRef): Comparable {
        throw unbound(rangeRef)
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess): Comparable {
        return getOrUnbound(fieldAccess)
    }

    @Override
    fun visitSubQuery(subQuery: RexSubQuery): Comparable {
        throw unbound(subQuery)
    }

    @Override
    fun visitTableInputRef(fieldRef: RexTableInputRef): Comparable {
        throw unbound(fieldRef)
    }

    @Override
    fun visitPatternFieldRef(fieldRef: RexPatternFieldRef): Comparable {
        throw unbound(fieldRef)
    }

    @Override
    fun visitCall(call: RexCall): Comparable {
        val values: List<Comparable> = visitList(call.operands)
        return when (call.getKind()) {
            IS_NOT_DISTINCT_FROM -> {
                if (containsNull(values)) {
                    values[0].equals(values[1])
                } else compare(values, IntPredicate { c -> c === 0 })
            }
            EQUALS -> compare(values, IntPredicate { c -> c === 0 })
            IS_DISTINCT_FROM -> {
                if (containsNull(values)) {
                    !values[0].equals(values[1])
                } else compare(values, IntPredicate { c -> c !== 0 })
            }
            NOT_EQUALS -> compare(values, IntPredicate { c -> c !== 0 })
            GREATER_THAN -> compare(values, IntPredicate { c -> c > 0 })
            GREATER_THAN_OR_EQUAL -> compare(values, IntPredicate { c -> c >= 0 })
            LESS_THAN -> compare(values, IntPredicate { c -> c < 0 })
            LESS_THAN_OR_EQUAL -> compare(values, IntPredicate { c -> c <= 0 })
            AND -> values.stream().map { c: Comparable -> Truthy.of(c) }.min(Comparator.naturalOrder())
                .get().toComparable()
            OR -> values.stream().map { c: Comparable -> Truthy.of(c) }.max(Comparator.naturalOrder())
                .get().toComparable()
            NOT -> not(values[0])
            CASE -> case_(values)
            IS_TRUE -> values[0].equals(true)
            IS_NOT_TRUE -> !values[0].equals(true)
            IS_NULL -> values[0].equals(N)
            IS_NOT_NULL -> !values[0].equals(N)
            IS_FALSE -> values[0].equals(false)
            IS_NOT_FALSE -> !values[0].equals(false)
            PLUS_PREFIX -> values[0]
            MINUS_PREFIX -> if (containsNull(values)) N else number(
                values[0]
            ).negate()
            PLUS -> if (containsNull(values)) N else number(
                values[0]
            ).add(number(values[1]))
            MINUS -> if (containsNull(values)) N else number(
                values[0]
            ).subtract(number(values[1]))
            TIMES -> if (containsNull(values)) N else number(
                values[0]
            ).multiply(number(values[1]))
            DIVIDE -> if (containsNull(values)) N else number(
                values[0]
            ).divide(number(values[1]))
            CAST -> cast(values)
            COALESCE -> coalesce(values)
            CEIL, FLOOR -> ceil(call, values)
            EXTRACT -> extract(values)
            LIKE -> like(values)
            SIMILAR -> similar(values)
            SEARCH -> search(call.operands.get(1).getType().getSqlTypeName(), values)
            else -> throw unbound(call)
        }
    }

    /** An enum that wraps boolean and unknown values and makes them
     * comparable.  */
    internal enum class Truthy {
        // Order is important; AND returns the min, OR returns the max
        FALSE, UNKNOWN, TRUE;

        fun toComparable(): Comparable {
            return when (this) {
                TRUE -> true
                FALSE -> false
                UNKNOWN -> N
                else -> throw AssertionError()
            }
        }

        companion object {
            fun of(c: Comparable): Truthy {
                return if (c.equals(true)) TRUE else if (c.equals(false)) FALSE else UNKNOWN
            }
        }
    }

    companion object {
        private val N: NullSentinel = NullSentinel.INSTANCE
        val SUPPORTED_SQL_KIND: EnumSet<SqlKind> = EnumSet.of(
            SqlKind.IS_NOT_DISTINCT_FROM, SqlKind.EQUALS, SqlKind.IS_DISTINCT_FROM,
            SqlKind.NOT_EQUALS, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.AND, SqlKind.OR,
            SqlKind.NOT, SqlKind.CASE, SqlKind.IS_TRUE, SqlKind.IS_NOT_TRUE,
            SqlKind.IS_FALSE, SqlKind.IS_NOT_FALSE, SqlKind.PLUS_PREFIX,
            SqlKind.MINUS_PREFIX, SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES,
            SqlKind.DIVIDE, SqlKind.COALESCE, SqlKind.CEIL,
            SqlKind.FLOOR, SqlKind.EXTRACT
        )

        /** Evaluates an expression in an environment.  */
        @Nullable
        fun evaluate(e: RexNode, map: Map<RexNode, Comparable>): Comparable {
            val v: Comparable = e.accept(RexInterpreter(map))
            if (false) {
                System.out.println("evaluate $e on $map returns $v")
            }
            return v
        }

        private fun unbound(e: RexNode): IllegalArgumentException {
            return IllegalArgumentException("unbound: $e")
        }

        private fun extract(values: List<Comparable>): Comparable {
            val v: Comparable = values[1]
            if (v === N) {
                return N
            }
            val timeUnitRange: TimeUnitRange = values[0] as TimeUnitRange
            val v2: Int
            v2 = if (v is Long) {
                // TIMESTAMP
                (v as Long / TimeUnit.DAY.multiplier.longValue()) as Int
            } else {
                // DATE
                v as Integer
            }
            return DateTimeUtils.unixDateExtract(timeUnitRange, v2)
        }

        private fun like(values: List<Comparable>): Comparable {
            if (containsNull(values)) {
                return N
            }
            val value: NlsString = values[0] as NlsString
            val pattern: NlsString = values[1] as NlsString
            return when (values.size()) {
                2 -> SqlFunctions.like(value.getValue(), pattern.getValue())
                3 -> {
                    val escape: NlsString = values[2] as NlsString
                    SqlFunctions.like(
                        value.getValue(), pattern.getValue(),
                        escape.getValue()
                    )
                }
                else -> throw AssertionError()
            }
        }

        private fun similar(values: List<Comparable>): Comparable {
            if (containsNull(values)) {
                return N
            }
            val value: NlsString = values[0] as NlsString
            val pattern: NlsString = values[1] as NlsString
            return when (values.size()) {
                2 -> SqlFunctions.similar(value.getValue(), pattern.getValue())
                3 -> {
                    val escape: NlsString = values[2] as NlsString
                    SqlFunctions.similar(
                        value.getValue(), pattern.getValue(),
                        escape.getValue()
                    )
                }
                else -> throw AssertionError()
            }
        }

        @SuppressWarnings(["BetaApi", "rawtypes", "unchecked", "UnstableApiUsage"])
        private fun search(typeName: SqlTypeName, values: List<Comparable>): Comparable {
            val value: Comparable = values[0]
            val sarg: Sarg = values[1] as Sarg
            return if (value === N) {
                when (sarg.nullAs) {
                    FALSE -> false
                    TRUE -> true
                    else -> N
                }
            } else translate(sarg.rangeSet, typeName)
                .contains(value)
        }

        /** Translates the values in a RangeSet from literal format to runtime format.
         * For example the DATE SQL type uses DateString for literals and Integer at
         * runtime.  */
        @SuppressWarnings(["BetaApi", "rawtypes", "unchecked", "UnstableApiUsage"])
        private fun translate(rangeSet: RangeSet, typeName: SqlTypeName): RangeSet {
            return when (typeName) {
                DATE -> RangeSets.copy(rangeSet, DateString::getDaysSinceEpoch)
                TIME -> RangeSets.copy(rangeSet, TimeString::getMillisOfDay)
                TIMESTAMP -> RangeSets.copy(rangeSet, TimestampString::getMillisSinceEpoch)
                else -> rangeSet
            }
        }

        private fun coalesce(values: List<Comparable>): Comparable {
            for (value in values) {
                if (value !== N) {
                    return value
                }
            }
            return N
        }

        private fun ceil(call: RexCall, values: List<Comparable>): Comparable {
            if (values[0] === N) {
                return N
            }
            val v = values[0] as Long
            val unit: TimeUnitRange = values[1] as TimeUnitRange
            when (unit) {
                YEAR, MONTH -> return when (call.getKind()) {
                    FLOOR -> DateTimeUtils.unixTimestampFloor(unit, v)
                    else -> DateTimeUtils.unixTimestampCeil(unit, v)
                }
                else -> {}
            }
            val subUnit: TimeUnitRange = subUnit(unit)
            var v2 = v
            while (true) {
                val e: Int = DateTimeUtils.unixTimestampExtract(subUnit, v2)
                if (e == 0) {
                    return v2
                }
                v2 -= unit.startUnit.multiplier.longValue()
            }
        }

        private fun subUnit(unit: TimeUnitRange): TimeUnitRange {
            return when (unit) {
                QUARTER -> TimeUnitRange.MONTH
                else -> TimeUnitRange.DAY
            }
        }

        private fun cast(values: List<Comparable>): Comparable {
            return if (values[0] === N) {
                N
            } else values[0]
        }

        private fun not(value: Comparable): Comparable {
            return if (value.equals(true)) {
                false
            } else if (value.equals(false)) {
                true
            } else {
                N
            }
        }

        private fun case_(values: List<Comparable>): Comparable {
            val size: Int
            val elseValue: Comparable
            if (values.size() % 2 === 0) {
                size = values.size()
                elseValue = N
            } else {
                size = values.size() - 1
                elseValue = Util.last(values)
            }
            var i = 0
            while (i < size) {
                if (values[i].equals(true)) {
                    return values[i + 1]
                }
                i += 2
            }
            return elseValue
        }

        private fun number(comparable: Comparable): BigDecimal {
            return if (comparable is BigDecimal) comparable as BigDecimal else if (comparable is BigInteger) BigDecimal(
                comparable as BigInteger
            ) else if (comparable is Long
                || comparable is Integer
                || comparable is Short
            ) BigDecimal((comparable as Number).longValue()) else BigDecimal((comparable as Number).doubleValue())
        }

        private fun compare(values: List<Comparable>, p: IntPredicate): Comparable {
            if (containsNull(values)) {
                return N
            }
            var v0: Comparable = values[0]
            var v1: Comparable = values[1]
            if (v0 is Number && v1 is NlsString) {
                try {
                    v1 = BigDecimal((v1 as NlsString).getValue())
                } catch (e: NumberFormatException) {
                    return false
                }
            }
            if (v1 is Number && v0 is NlsString) {
                try {
                    v0 = BigDecimal((v0 as NlsString).getValue())
                } catch (e: NumberFormatException) {
                    return false
                }
            }
            if (v0 is Number) {
                v0 = number(v0)
            }
            if (v1 is Number) {
                v1 = number(v1)
            }
            val c: Int = v0.compareTo(v1)
            return p.test(c)
        }

        private fun containsNull(values: List<Comparable>): Boolean {
            for (value in values) {
                if (value === N) {
                    return true
                }
            }
            return false
        }
    }
}
