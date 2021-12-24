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
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.BitString
import org.apache.calcite.util.Util
import java.util.List
import java.util.Objects

/**
 * A binary (or hexadecimal) string literal.
 *
 *
 * The [.value] field is a [BitString] and [.getTypeName]
 * is [SqlTypeName.BINARY].
 */
class SqlBinaryStringLiteral  //~ Constructors -----------------------------------------------------------
protected constructor(
    `val`: BitString?,
    pos: SqlParserPos?
) : SqlAbstractStringLiteral(`val`, SqlTypeName.BINARY, pos) {
    //~ Methods ----------------------------------------------------------------// to be removed before 2.0
    /** Returns the underlying [BitString].
     *
     */
    @get:Deprecated("Use {@link SqlLiteral#getValueAs getValueAs(BitString.class)}")
    @get:Deprecated
    val bitString: BitString

    // to be removed before 2.0 get() = valueNonNull
    private val valueNonNull: BitString
        private get() = Objects.requireNonNull(value, "value") as BitString

    @Override
    fun clone(pos: SqlParserPos?): SqlBinaryStringLiteral {
        return SqlBinaryStringLiteral(valueNonNull, pos)
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.literal("X'" + valueNonNull.toHexString().toString() + "'")
    }

    @Override
    protected fun concat1(literals: List<SqlLiteral>): SqlAbstractStringLiteral {
        return SqlBinaryStringLiteral(
            BitString.concat(
                Util.transform(
                    literals
                ) { literal -> literal.getValueAs(BitString::class.java) }),
            literals[0].getParserPosition()
        )
    }
}
