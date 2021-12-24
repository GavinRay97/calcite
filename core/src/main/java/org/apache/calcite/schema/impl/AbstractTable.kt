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
package org.apache.calcite.schema.impl

import org.apache.calcite.config.CalciteConnectionConfig

/**
 * Abstract base class for implementing [Table].
 *
 *
 * Sub-classes should override [.isRolledUp] and
 * [Table.rolledUpColumnValidInsideAgg]
 * if their table can potentially contain rolled up values. This information is
 * used by the validator to check for illegal uses of these columns.
 */
abstract class AbstractTable protected constructor() : Table, Wrapper {
    // Default implementation. Override if you have statistics.
    @get:Override
    val statistic: Statistic
        get() = Statistics.UNKNOWN

    @get:Override
    val jdbcTableType: Schema.TableType
        get() = Schema.TableType.TABLE

    @Override
    fun <C : Object?> unwrap(aClass: Class<C>): @Nullable C? {
        return if (aClass.isInstance(this)) {
            aClass.cast(this)
        } else null
    }

    @Override
    fun isRolledUp(column: String?): Boolean {
        return false
    }

    @Override
    fun rolledUpColumnValidInsideAgg(
        column: String?,
        call: SqlCall?, @Nullable parent: SqlNode?, @Nullable config: CalciteConnectionConfig?
    ): Boolean {
        return true
    }
}
