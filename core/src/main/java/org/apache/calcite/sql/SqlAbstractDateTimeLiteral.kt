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

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.TimestampString
import java.util.Objects.requireNonNull

/**
 * A SQL literal representing a DATE, TIME or TIMESTAMP value.
 *
 *
 * Examples:
 *
 *
 *  * DATE '2004-10-22'
 *  * TIME '14:33:44.567'
 *  * `TIMESTAMP '1969-07-21 03:15 GMT'`
 *
 */
abstract class SqlAbstractDateTimeLiteral
/**
 * Constructs a datetime literal.
 */ protected constructor(
    d: Object?, //~ Instance fields --------------------------------------------------------
    protected val hasTimeZone: Boolean,
    typeName: SqlTypeName?, val prec: Int, pos: SqlParserPos?
) : SqlLiteral(d, typeName, pos) {
    //~ Constructors -----------------------------------------------------------
    //~ Methods ----------------------------------------------------------------
    /** Converts this literal to a [TimestampString].  */
    protected val timestamp: TimestampString
        protected get() = requireNonNull(value, "value") as TimestampString

    /**
     * Returns e.g. `DATE '1969-07-21'`.
     */
    @Override
    abstract override fun toString(): String

    /**
     * Returns e.g. `1969-07-21`.
     */
    abstract fun toFormattedString(): String?
    @Override
    fun createSqlType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.createSqlType(
            getTypeName(),
            prec
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.literal(this.toString())
    }
}
