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

import java.util.EnumSet
import java.util.Locale

/**
 * SqlAccessType is represented by a set of allowed access types.
 *
 * @see SqlAccessEnum
 */
class SqlAccessType(accessEnums: EnumSet<SqlAccessEnum?>) {
    //~ Instance fields --------------------------------------------------------
    private val accessEnums: EnumSet<SqlAccessEnum>

    //~ Constructors -----------------------------------------------------------
    init {
        this.accessEnums = accessEnums
    }

    //~ Methods ----------------------------------------------------------------
    fun allowsAccess(access: SqlAccessEnum?): Boolean {
        return accessEnums.contains(access)
    }

    @Override
    override fun toString(): String {
        return accessEnums.toString()
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val ALL = SqlAccessType(
            EnumSet.allOf(
                SqlAccessEnum::class.java
            )
        )
        val READ_ONLY = SqlAccessType(EnumSet.of(SqlAccessEnum.SELECT))
        val WRITE_ONLY = SqlAccessType(EnumSet.of(SqlAccessEnum.INSERT))
        fun create(accessNames: Array<String>?): SqlAccessType {
            assert(accessNames != null)
            val enumSet: EnumSet<SqlAccessEnum?> = EnumSet.noneOf(SqlAccessEnum::class.java)
            for (accessName in accessNames!!) {
                enumSet.add(
                    SqlAccessEnum.valueOf(accessName.trim().toUpperCase(Locale.ROOT))
                )
            }
            return SqlAccessType(enumSet)
        }

        fun create(accessString: String?): SqlAccessType {
            var accessString = accessString
            assert(accessString != null)
            accessString = accessString.replace('[', ' ')
            accessString = accessString.replace(']', ' ')
            val accessNames: Array<String> = accessString.split(",")
            return create(accessNames)
        }
    }
}
