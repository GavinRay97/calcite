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

import org.apache.calcite.rel.type.RelDataType

/**
 * Scope for expressions in a `MATCH_RECOGNIZE` clause.
 *
 *
 * Defines variables and uses them as prefix of columns reference.
 */
class MatchRecognizeScope(
    parent: SqlValidatorScope?,
    matchRecognize: SqlMatchRecognize
) : ListScope(parent) {
    //~ Instance fields ---------------------------------------------
    private val matchRecognize: SqlMatchRecognize
    val patternVars: Set<String>

    /** Creates a MatchRecognizeScope.  */
    init {
        this.matchRecognize = matchRecognize
        patternVars = validator.getCatalogReader().nameMatcher().createSet()
        patternVars.add(STAR)
    }

    @get:Override
    val node: SqlNode
        get() = matchRecognize

    fun getMatchRecognize(): SqlMatchRecognize {
        return matchRecognize
    }

    fun addPatternVar(str: String?) {
        patternVars.add(str)
    }

    @Override
    override fun findQualifyingTableNames(
        columnName: String?, ctx: SqlNode?,
        nameMatcher: SqlNameMatcher
    ): Map<String, ScopeChild> {
        val map: Map<String, ScopeChild> = HashMap()
        for (child in children) {
            val rowType: RelDataType = child.namespace.getRowType()
            if (nameMatcher.field(rowType, columnName) != null) {
                map.put(STAR, child)
            }
        }
        return when (map.size()) {
            0 -> parent.findQualifyingTableNames(columnName, ctx, nameMatcher)
            else -> map
        }
    }

    @Override
    override fun resolve(
        names: List<String>, nameMatcher: SqlNameMatcher,
        deep: Boolean, resolved: Resolved
    ) {
        if (patternVars.contains(names[0])) {
            val path: Step = EmptyPath().plus(null, 0, "", StructKind.FULLY_QUALIFIED)
            val child: ScopeChild = children.get(0)
            resolved.found(child.namespace, child.nullable, this, path, names)
            if (resolved.count() > 0) {
                return
            }
        }
        super.resolve(names, nameMatcher, deep, resolved)
    }

    companion object {
        private const val STAR = "*"
    }
}
