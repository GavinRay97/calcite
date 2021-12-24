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

import org.apache.calcite.sql.SqlNode

/**
 * The name-resolution scope of a LATERAL TABLE clause.
 *
 *
 * The objects visible are those in the parameters found on the left side of
 * the LATERAL TABLE clause, and objects inherited from the parent scope.
 */
internal class TableScope(parent: SqlValidatorScope?, node: SqlNode?) :
    ListScope(Objects.requireNonNull(parent, "parent")) {
    //~ Instance fields --------------------------------------------------------
    private val node: SqlNode
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a scope corresponding to a LATERAL TABLE clause.
     *
     * @param parent  Parent scope
     */
    init {
        this.node = Objects.requireNonNull(node, "node")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getNode(): SqlNode {
        return node
    }

    @Override
    fun isWithin(@Nullable scope2: SqlValidatorScope): Boolean {
        if (this === scope2) {
            return true
        }
        val s: SqlValidatorScope = getValidator().getSelectScope(node as SqlSelect)
        return s.isWithin(scope2)
    }
}
