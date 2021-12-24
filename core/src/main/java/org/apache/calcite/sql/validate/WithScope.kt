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

import org.apache.calcite.rel.type.StructKind

/** Scope providing the objects that are available after evaluating an item
 * in a WITH clause.
 *
 *
 * For example, in
 *
 * <blockquote>`WITH t1 AS (q1) t2 AS (q2) q3`</blockquote>
 *
 *
 * `t1` provides a scope that is used to validate `q2`
 * (and therefore `q2` may reference `t1`),
 * and `t2` provides a scope that is used to validate `q3`
 * (and therefore q3 may reference `t1` and `t2`).
 */
internal class WithScope(parent: SqlValidatorScope?, withItem: SqlWithItem) : ListScope(parent) {
    private val withItem: SqlWithItem

    /** Creates a WithScope.  */
    init {
        this.withItem = withItem
    }

    @get:Override
    val node: SqlNode
        get() = withItem

    @Override
    @Nullable
    fun getTableNamespace(names: List<String>): SqlValidatorNamespace {
        return if (names.size() === 1 && names[0].equals(withItem.name.getSimple())) {
            validator.getNamespace(withItem)
        } else super.getTableNamespace(names)
    }

    @Override
    fun resolveTable(
        names: List<String?>,
        nameMatcher: SqlNameMatcher?, path: Path, resolved: Resolved
    ) {
        if (names.size() === 1
            && names.equals(withItem.name.names)
        ) {
            val ns: SqlValidatorNamespace = validator.getNamespaceOrThrow(withItem)
            val path2: Step = path
                .plus(ns.getRowType(), 0, names[0], StructKind.FULLY_QUALIFIED)
            resolved.found(ns, false, null, path2, null)
            return
        }
        super.resolveTable(names, nameMatcher, path, resolved)
    }
}
