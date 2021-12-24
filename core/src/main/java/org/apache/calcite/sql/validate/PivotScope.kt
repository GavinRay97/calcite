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
package org.apache.calcite.sql.validate

import org.apache.calcite.sql.SqlPivot

/**
 * Scope for expressions in a `PIVOT` clause.
 */
class PivotScope(parent: SqlValidatorScope?, pivot: SqlPivot) : ListScope(parent) {
    //~ Instance fields ---------------------------------------------
    private val pivot: SqlPivot

    /** Creates a PivotScope.  */
    init {
        this.pivot = pivot
    }

    /** By analogy with
     * [org.apache.calcite.sql.validate.ListScope.getChildren], but this
     * scope only has one namespace, and it is anonymous.  */
    val child: SqlValidatorNamespace
        get() = requireNonNull(
            validator.getNamespace(pivot.query)
        ) { "namespace for pivot.query " + pivot.query }

    @get:Override
    val node: SqlPivot
        get() = pivot
}
