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

import org.apache.calcite.sql.SqlUnpivot

/**
 * Scope for expressions in an `UNPIVOT` clause.
 */
class UnpivotScope(parent: SqlValidatorScope?, unpivot: SqlUnpivot) : ListScope(parent) {
    //~ Instance fields ---------------------------------------------
    private val unpivot: SqlUnpivot

    /** Creates an UnpivotScope.  */
    init {
        this.unpivot = unpivot
    }

    /** By analogy with
     * [ListScope.getChildren], but this
     * scope only has one namespace, and it is anonymous.  */
    val child: org.apache.calcite.sql.validate.SqlValidatorNamespace
        get() = requireNonNull(
            validator.getNamespace(unpivot.query)
        ) { "namespace for unpivot.query " + unpivot.query }

    @get:Override
    val node: SqlUnpivot
        get() = unpivot
}
