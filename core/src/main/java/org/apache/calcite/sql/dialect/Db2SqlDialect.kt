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
package org.apache.calcite.sql.dialect

import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlIntervalLiteral
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlWriter

/**
 * A `SqlDialect` implementation for the IBM DB2 database.
 */
class Db2SqlDialect
/** Creates a Db2SqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun hasImplicitTableAlias(): Boolean {
        return false
    }

    @Override
    fun unparseSqlIntervalQualifier(
        writer: SqlWriter,
        qualifier: SqlIntervalQualifier, typeSystem: RelDataTypeSystem?
    ) {

        // DB2 supported qualifiers. Singular form of these keywords are also acceptable.
        // YEAR/YEARS
        // MONTH/MONTHS
        // DAY/DAYS
        // HOUR/HOURS
        // MINUTE/MINUTES
        // SECOND/SECONDS
        when (qualifier.timeUnitRange) {
            YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MICROSECOND -> {
                val timeUnit: String = qualifier.timeUnitRange.startUnit.name()
                writer.keyword(timeUnit)
            }
            else -> throw AssertionError("Unsupported type: " + qualifier.timeUnitRange)
        }
        if (null != qualifier.timeUnitRange.endUnit) {
            throw AssertionError(
                "Unsupported end unit: "
                        + qualifier.timeUnitRange.endUnit
            )
        }
    }

    @Override
    fun unparseSqlIntervalLiteral(
        writer: SqlWriter,
        literal: SqlIntervalLiteral, leftPrec: Int, rightPrec: Int
    ) {
        // A duration is a positive or negative number representing an interval of time.
        // If one operand is a date, the other labeled duration of YEARS, MONTHS, or DAYS.
        // If one operand is a time, the other must be labeled duration of HOURS, MINUTES, or SECONDS.
        // If one operand is a timestamp, the other operand can be any of teh duration.
        val interval: SqlIntervalLiteral.IntervalValue =
            literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
        if (interval.getSign() === -1) {
            writer.print("-")
        }
        writer.literal(interval.getIntervalLiteral())
        unparseSqlIntervalQualifier(
            writer, interval.getIntervalQualifier(),
            RelDataTypeSystem.DEFAULT
        )
    }

    companion object {
        val DEFAULT_CONTEXT: SqlDialect.Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.DB2)
        val DEFAULT: SqlDialect = Db2SqlDialect(DEFAULT_CONTEXT)
    }
}
