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
import org.apache.calcite.util.Bug
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.Util
import java.util.List
import java.util.Objects

/**
 * A character string literal.
 *
 *
 * Its [.value] field is an [NlsString] and
 * [typeName][.getTypeName] is [SqlTypeName.CHAR].
 */
class SqlCharStringLiteral  //~ Constructors -----------------------------------------------------------
protected constructor(`val`: NlsString?, pos: SqlParserPos?) : SqlAbstractStringLiteral(`val`, SqlTypeName.CHAR, pos) {
    //~ Methods ----------------------------------------------------------------// to be removed before 2.0
    /**
     * Returns the underlying NlsString.
     *
     */
    @get:Deprecated("Use {@link #getValueAs getValueAs(NlsString.class)}")
    @get:Deprecated
    val nlsString: NlsString
    // to be removed before 2.0 get() {
    return valueNonNull
}

private val valueNonNull: NlsString
    private get() = Objects.requireNonNull(value, "value") as NlsString

/**
 * Returns the collation.
 */
@get:Nullable
val collation: org.apache.calcite.sql.SqlCollation
    @Nullable get() = valueNonNull.getCollation()

@Override
fun clone(pos: SqlParserPos?): SqlCharStringLiteral {
    return SqlCharStringLiteral(valueNonNull, pos)
}

@Override
fun unparse(
    writer: SqlWriter,
    leftPrec: Int,
    rightPrec: Int
) {
    val nlsString: NlsString = valueNonNull
    if (false) {
        Util.discard(Bug.FRG78_FIXED)
        val stringValue: String = nlsString.getValue()
        writer.literal(
            writer.getDialect().quoteStringLiteral(stringValue)
        )
    }
    writer.literal(nlsString.asSql(true, true, writer.getDialect()))
}

@Override
protected fun concat1(literals: List<SqlLiteral>): SqlAbstractStringLiteral {
    return SqlCharStringLiteral(
        NlsString.concat(
            Util.transform(
                literals
            ) { literal -> literal.getValueAs(NlsString::class.java) }),
        literals[0].getParserPosition()
    )
}
}
