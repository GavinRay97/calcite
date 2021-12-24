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
package org.apache.calcite.sql.type

import org.apache.calcite.avatica.util.TimeUnit

/**
 * IntervalSqlType represents a standard SQL datetime interval type.
 */
class IntervalSqlType(
    typeSystem: RelDataTypeSystem?,
    intervalQualifier: SqlIntervalQualifier,
    isNullable: Boolean
) : AbstractSqlType(intervalQualifier.typeName(), isNullable, null) {
    //~ Instance fields --------------------------------------------------------
    private val typeSystem: RelDataTypeSystem
    private val intervalQualifier: SqlIntervalQualifier
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs an IntervalSqlType. This should only be called from a factory
     * method.
     */
    init {
        this.typeSystem = Objects.requireNonNull(typeSystem, "typeSystem")
        this.intervalQualifier = Objects.requireNonNull(intervalQualifier, "intervalQualifier")
        computeDigest()
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        sb.append("INTERVAL ")
        val dialect: SqlDialect = AnsiSqlDialect.DEFAULT
        val config: SqlWriterConfig = SqlPrettyWriter.config()
            .withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withIndentation(0)
            .withDialect(dialect)
        val writer = SqlPrettyWriter(config)
        intervalQualifier.unparse(writer, 0, 0)
        val sql: String = writer.toString()
        sb.append(SqlString(dialect, sql).getSql())
    }

    @Override
    fun getIntervalQualifier(): SqlIntervalQualifier {
        return intervalQualifier
    }

    /**
     * Combines two IntervalTypes and returns the result. E.g. the result of
     * combining<br></br>
     * `INTERVAL DAY TO HOUR`<br></br>
     * with<br></br>
     * `INTERVAL SECOND` is<br></br>
     * `INTERVAL DAY TO SECOND`
     */
    fun combine(
        typeFactory: RelDataTypeFactoryImpl,
        that: IntervalSqlType?
    ): IntervalSqlType? {
        assert(this.typeName.isYearMonth() === that.typeName.isYearMonth())
        val nullable = isNullable || that!!.isNullable
        var thisStart: TimeUnit = Objects.requireNonNull(typeName.getStartUnit())
        var thisEnd: TimeUnit? = typeName.getEndUnit()
        val thatStart: TimeUnit = Objects.requireNonNull(that!!.typeName.getStartUnit())
        val thatEnd: TimeUnit = that!!.typeName.getEndUnit()
        var secondPrec: Int = intervalQualifier.getStartPrecisionPreservingDefault()
        val fracPrec: Int = SqlIntervalQualifier.combineFractionalSecondPrecisionPreservingDefault(
            typeSystem,
            intervalQualifier,
            that.intervalQualifier
        )
        if (thisStart.ordinal() > thatStart.ordinal()) {
            thisEnd = thisStart
            thisStart = thatStart
            secondPrec = that.intervalQualifier.getStartPrecisionPreservingDefault()
        } else if (thisStart.ordinal() === thatStart.ordinal()) {
            secondPrec = SqlIntervalQualifier.combineStartPrecisionPreservingDefault(
                typeFactory.getTypeSystem(),
                intervalQualifier,
                that.intervalQualifier
            )
        } else if (null == thisEnd || thisEnd.ordinal() < thatStart.ordinal()) {
            thisEnd = thatStart
        }
        if (null != thatEnd) {
            if (null == thisEnd || thisEnd.ordinal() < thatEnd.ordinal()) {
                thisEnd = thatEnd
            }
        }
        var intervalType: RelDataType = typeFactory.createSqlIntervalType(
            SqlIntervalQualifier(
                thisStart,
                secondPrec,
                thisEnd,
                fracPrec,
                SqlParserPos.ZERO
            )
        )
        intervalType = typeFactory.createTypeWithNullability(
            intervalType,
            nullable
        )
        return intervalType
    }

    @get:Override
    val precision: Int
        get() = intervalQualifier.getStartPrecision(typeSystem)

    @get:Override
    val scale: Int
        get() = intervalQualifier.getFractionalSecondPrecision(typeSystem)
}
