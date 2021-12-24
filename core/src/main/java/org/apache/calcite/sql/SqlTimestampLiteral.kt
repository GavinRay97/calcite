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
 * A SQL literal representing a TIMESTAMP value, for example `TIMESTAMP
 * '1969-07-21 03:15 GMT'`.
 *
 *
 * Create values using [SqlLiteral.createTimestamp].
 */
class SqlTimestampLiteral internal constructor(
    ts: TimestampString?, precision: Int,
    hasTimeZone: Boolean, pos: SqlParserPos?
) : SqlAbstractDateTimeLiteral(ts, hasTimeZone, SqlTypeName.TIMESTAMP, precision, pos) {
    //~ Constructors -----------------------------------------------------------
    init {
        Preconditions.checkArgument(precision >= 0)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun clone(pos: SqlParserPos?): SqlTimestampLiteral {
        return SqlTimestampLiteral(
            Objects.requireNonNull(value, "value") as TimestampString,
            precision,
            hasTimeZone, pos
        )
    }

    @Override
    override fun toString(): String {
        return "TIMESTAMP '" + toFormattedString() + "'"
    }

    /**
     * Returns e.g. '03:05:67.456'.
     */
    @Override
    fun toFormattedString(): String {
        var ts: TimestampString = getTimestamp()
        if (precision > 0) {
            ts = ts.round(precision)
        }
        return ts.toString(precision)
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.getDialect().unparseDateTimeLiteral(writer, this, leftPrec, rightPrec)
    }
}
