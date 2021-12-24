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

import org.apache.calcite.sql.SqlJoin

/**
 * The name-resolution context for expression inside a JOIN clause. The objects
 * visible are the joined table expressions, and those inherited from the parent
 * scope.
 *
 *
 * Consider "SELECT * FROM (A JOIN B ON {exp1}) JOIN C ON {exp2}". {exp1} is
 * resolved in the join scope for "A JOIN B", which contains A and B but not
 * C.
 */
class JoinScope internal constructor(
    parent: SqlValidatorScope?,
    @Nullable usingScope: SqlValidatorScope?,
    join: SqlJoin
) : ListScope(parent) {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    private val usingScope: SqlValidatorScope?
    private val join: SqlJoin
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `JoinScope`.
     *
     * @param parent     Parent scope
     * @param usingScope Scope for resolving USING clause
     * @param join       Call to JOIN operator
     */
    init {
        this.usingScope = usingScope
        this.join = join
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val node: SqlNode
        get() = join

    @Override
    override fun addChild(
        ns: SqlValidatorNamespace?, alias: String?,
        nullable: Boolean
    ) {
        super.addChild(ns, alias, nullable)
        if (usingScope != null && usingScope !== parent) {
            // We're looking at a join within a join. Recursively add this
            // child to its parent scope too. Example:
            //
            //   select *
            //   from (a join b on expr1)
            //   join c on expr2
            //   where expr3
            //
            // 'a' is a child namespace of 'a join b' and also of
            // 'a join b join c'.
            usingScope.addChild(ns, alias, nullable)
        }
    }

    @Override
    @Nullable
    fun lookupWindow(name: String?): SqlWindow? {
        // Lookup window in enclosing select.
        return if (usingScope != null) {
            usingScope.lookupWindow(name)
        } else {
            null
        }
    }

    /**
     * Returns the scope which is used for resolving USING clause.
     */
    @Nullable
    fun getUsingScope(): SqlValidatorScope? {
        return usingScope
    }

    @Override
    fun isWithin(@Nullable scope2: SqlValidatorScope): Boolean {
        return if (this === scope2) {
            true
        } else requireNonNull(usingScope, "usingScope").isWithin(scope2)
        // go from the JOIN to the enclosing SELECT
    }
}
