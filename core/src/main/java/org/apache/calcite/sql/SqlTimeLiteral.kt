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
 * A SQL literal representing a TIME value, for example `TIME
 * '14:33:44.567'`.
 *
 *
 * Create values using [SqlLiteral.createTime].
 */
class SqlTimeLiteral internal constructor(
    t: TimeString?, precision: Int, hasTimeZone: Boolean,
    pos: SqlParserPos?
) : SqlAbstractDateTimeLiteral(t, hasTimeZone, SqlTypeName.TIME, precision, pos) {
    //~ Constructors -----------------------------------------------------------
    init {
        Preconditions.checkArgument(precision >= 0)
    }
    //~ Methods ----------------------------------------------------------------
    /** Converts this literal to a [TimeString].  */
    protected val time: TimeString
        protected get() = Objects.requireNonNull(value, "value") as TimeString

    @Override
    fun clone(pos: SqlParserPos?): SqlTimeLiteral {
        return SqlTimeLiteral(time, precision, hasTimeZone, pos)
    }

    @Override
    override fun toString(): String {
        return "TIME '" + toFormattedString() + "'"
    }

    /**
     * Returns e.g. '03:05:67.456'.
     */
    @Override
    fun toFormattedString(): String {
        return time.toString(precision)
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
