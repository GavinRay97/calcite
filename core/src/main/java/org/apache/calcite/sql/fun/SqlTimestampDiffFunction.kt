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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.avatica.util.TimeUnit

/**
 * The `TIMESTAMPDIFF` function, which calculates the difference
 * between two timestamps.
 *
 *
 * The SQL syntax is
 *
 * <blockquote>
 * `TIMESTAMPDIFF(*timestamp interval*, *timestamp*,
 * *timestamp*)`
</blockquote> *
 *
 *
 * The interval time unit can one of the following literals:
 *  * NANOSECOND (and synonym SQL_TSI_FRAC_SECOND)
 *  * MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND)
 *  * SECOND (and synonym SQL_TSI_SECOND)
 *  * MINUTE (and synonym  SQL_TSI_MINUTE)
 *  * HOUR (and synonym  SQL_TSI_HOUR)
 *  * DAY (and synonym SQL_TSI_DAY)
 *  * WEEK (and synonym  SQL_TSI_WEEK)
 *  * MONTH (and synonym SQL_TSI_MONTH)
 *  * QUARTER (and synonym SQL_TSI_QUARTER)
 *  * YEAR (and synonym  SQL_TSI_YEAR)
 *
 *
 *
 * Returns difference between two timestamps in indicated timestamp
 * interval.
 */
internal object SqlTimestampDiffFunction : SqlFunction() {
    /** Creates a SqlTimestampDiffFunction.  */
    private val RETURN_TYPE_INFERENCE: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val sqlTypeName: SqlTypeName = if (opBinding.getOperandLiteralValue(
                0,
                TimeUnit::class.java
            ) === TimeUnit.NANOSECOND
        ) SqlTypeName.BIGINT else SqlTypeName.INTEGER
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(sqlTypeName), opBinding.getOperandType(1).isNullable()
                    || opBinding.getOperandType(2).isNullable()
        )
    }
}
