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

import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.util.DateString
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.TimeString
import org.apache.calcite.util.TimestampString
import java.util.Objects.requireNonNull

/**
 * Standard implementation of [RexToSqlNodeConverter].
 */
class RexToSqlNodeConverterImpl(convertletTable: RexSqlConvertletTable) : RexToSqlNodeConverter {
    //~ Instance fields --------------------------------------------------------
    private val convertletTable: RexSqlConvertletTable

    //~ Constructors -----------------------------------------------------------
    init {
        this.convertletTable = convertletTable
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    override fun convertNode(node: RexNode): SqlNode? {
        if (node is RexLiteral) {
            return convertLiteral(node as RexLiteral)
        } else if (node is RexInputRef) {
            return convertInputRef(node as RexInputRef)
        } else if (node is RexCall) {
            return convertCall(node as RexCall)
        }
        return null
    }

    // implement RexToSqlNodeConverter
    @Override
    @Nullable
    override fun convertCall(call: RexCall?): SqlNode? {
        val convertlet: RexSqlConvertlet = convertletTable.get(call)
        return if (convertlet != null) {
            convertlet.convertCall(this, call)
        } else null
    }

    @Override
    @Nullable
    override fun convertLiteral(literal: RexLiteral): SqlNode? {
        // Numeric
        if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createExactNumeric(
                String.valueOf(literal.getValue()),
                SqlParserPos.ZERO
            )
        }
        if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createApproxNumeric(
                String.valueOf(literal.getValue()),
                SqlParserPos.ZERO
            )
        }

        // Timestamp
        if (SqlTypeFamily.TIMESTAMP.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createTimestamp(
                requireNonNull(
                    literal.getValueAs(TimestampString::class.java),
                    "literal.getValueAs(TimestampString.class)"
                ),
                0,
                SqlParserPos.ZERO
            )
        }

        // Date
        if (SqlTypeFamily.DATE.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createDate(
                requireNonNull(
                    literal.getValueAs(DateString::class.java),
                    "literal.getValueAs(DateString.class)"
                ),
                SqlParserPos.ZERO
            )
        }

        // Time
        if (SqlTypeFamily.TIME.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createTime(
                requireNonNull(
                    literal.getValueAs(TimeString::class.java),
                    "literal.getValueAs(TimeString.class)"
                ),
                0,
                SqlParserPos.ZERO
            )
        }

        // String
        if (SqlTypeFamily.CHARACTER.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createCharString(
                requireNonNull(literal.getValue() as NlsString, "literal.getValue()")
                    .getValue(),
                SqlParserPos.ZERO
            )
        }

        // Boolean
        if (SqlTypeFamily.BOOLEAN.getTypeNames().contains(
                literal.getTypeName()
            )
        ) {
            return SqlLiteral.createBoolean(
                requireNonNull(literal.getValue(), "literal.getValue()") as Boolean?,
                SqlParserPos.ZERO
            )
        }

        // Null
        return if (SqlTypeFamily.NULL === literal.getTypeName().getFamily()) {
            SqlLiteral.createNull(SqlParserPos.ZERO)
        } else null
    }

    @Override
    @Nullable
    override fun convertInputRef(ref: RexInputRef?): SqlNode? {
        return null
    }
}
