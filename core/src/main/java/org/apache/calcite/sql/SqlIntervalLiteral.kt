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
package org.apache.calcite.sql

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * A SQL literal representing a time interval.
 *
 *
 * Examples:
 *
 *
 *  * INTERVAL '1' SECOND
 *  * INTERVAL '1:00:05.345' HOUR
 *  * INTERVAL '3:4' YEAR TO MONTH
 *
 *
 *
 * YEAR/MONTH intervals are not implemented yet.
 *
 *
 * The interval string, such as '1:00:05.345', is not parsed yet.
 */
class SqlIntervalLiteral private constructor(
    @Nullable intervalValue: IntervalValue,
    sqlTypeName: SqlTypeName,
    pos: SqlParserPos
) : SqlLiteral(
    intervalValue,
    sqlTypeName,
    pos
) {
    //~ Constructors -----------------------------------------------------------
    constructor(
        sign: Int,
        intervalStr: String,
        intervalQualifier: SqlIntervalQualifier?,
        sqlTypeName: SqlTypeName,
        pos: SqlParserPos
    ) : this(
        IntervalValue(intervalQualifier, sign, intervalStr),
        sqlTypeName,
        pos
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun clone(pos: SqlParserPos): SqlIntervalLiteral {
        return SqlIntervalLiteral(value as IntervalValue, getTypeName(), pos)
    }

    @Override
    override fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.getDialect().unparseSqlIntervalLiteral(writer, this, leftPrec, rightPrec)
    }

    @SuppressWarnings("deprecation")
    @Override
    override fun signum(): Int {
        return (castNonNull(value) as IntervalValue).signum()
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * A Interval value.
     */
    class IntervalValue internal constructor(
        intervalQualifier: SqlIntervalQualifier?,
        sign: Int,
        intervalStr: String
    ) {
        private val intervalQualifier: SqlIntervalQualifier?
        val intervalLiteral: String
        val sign: Int

        /**
         * Creates an interval value.
         *
         * @param intervalQualifier Interval qualifier
         * @param sign              Sign (+1 or -1)
         * @param intervalStr       Interval string
         */
        init {
            assert(sign == -1 || sign == 1)
            assert(intervalQualifier != null)
            assert(intervalStr != null)
            this.intervalQualifier = intervalQualifier
            this.sign = sign
            intervalLiteral = intervalStr
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            if (obj !is IntervalValue) {
                return false
            }
            val that = obj as IntervalValue
            return (intervalLiteral.equals(that.intervalLiteral)
                    && sign == that.sign
                    && intervalQualifier!!.equalsDeep(
                that.intervalQualifier,
                Litmus.IGNORE
            ))
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(sign, intervalLiteral, intervalQualifier)
        }

        fun getIntervalQualifier(): SqlIntervalQualifier? {
            return intervalQualifier
        }

        fun signum(): Int {
            for (i in 0 until intervalLiteral.length()) {
                val ch: Char = intervalLiteral.charAt(i)
                if (ch >= '1' && ch <= '9') {
                    // If non zero return sign.
                    return sign
                }
            }
            return 0
        }

        @Override
        override fun toString(): String {
            return intervalLiteral
        }
    }
}
