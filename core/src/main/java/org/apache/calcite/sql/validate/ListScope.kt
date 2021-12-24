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
 * Abstract base for a scope which is defined by a list of child namespaces and
 * which inherits from a parent scope.
 */
abstract class ListScope  //~ Constructors -----------------------------------------------------------
protected constructor(parent: SqlValidatorScope?) : DelegatingScope(parent) {
    //~ Instance fields --------------------------------------------------------
    /**
     * List of child [SqlValidatorNamespace] objects and their names.
     */
    val children: List<ScopeChild> = ArrayList()

    //~ Methods ----------------------------------------------------------------
    @Override
    fun addChild(
        ns: SqlValidatorNamespace?, alias: String?,
        nullable: Boolean
    ) {
        Objects.requireNonNull(alias, "alias")
        children.add(ScopeChild(children.size(), alias, ns, nullable))
    }

    /**
     * Returns an immutable list of child namespaces.
     *
     * @return list of child namespaces
     */
    fun getChildren(): List<SqlValidatorNamespace> {
        return Util.transform(children) { scopeChild -> scopeChild.namespace }
    }

    /**
     * Returns an immutable list of child names.
     *
     * @return list of child namespaces
     */
    val childNames: List<String>
        get() = Util.transform(children) { scopeChild -> scopeChild.name }

    @Nullable
    private fun findChild(
        names: List<String>,
        nameMatcher: SqlNameMatcher
    ): ScopeChild? {
        for (child in children) {
            val lastName: String = Util.last(names)
            if (child.name != null) {
                if (!nameMatcher.matches(child.name, lastName)) {
                    // Alias does not match last segment. Don't consider the
                    // fully-qualified name. E.g.
                    //    SELECT sales.emp.name FROM sales.emp AS otherAlias
                    continue
                }
                if (names.size() === 1) {
                    return child
                }
            }

            // Look up the 2 tables independently, in case one is qualified with
            // catalog & schema and the other is not.
            val table: SqlValidatorTable = child.namespace.getTable()
            if (table != null) {
                val resolved = ResolvedImpl()
                resolveTable(names, nameMatcher, Path.EMPTY, resolved)
                if (resolved.count() === 1) {
                    val only: Resolve = resolved.only()
                    val qualifiedName: List<String> = table.getQualifiedName()
                    if (only.remainingNames.isEmpty()
                        && only.namespace is TableNamespace
                        && Objects.equals(qualifiedName, getQualifiedName(only.namespace.getTable()))
                    ) {
                        return child
                    }
                }
            }
        }
        return null
    }

    @Override
    fun findAllColumnNames(result: List<SqlMoniker?>?) {
        for (child in children) {
            addColumnNames(child.namespace, result)
        }
        parent.findAllColumnNames(result)
    }

    @Override
    fun findAliases(result: Collection<SqlMoniker?>) {
        for (child in children) {
            result.add(SqlMonikerImpl(child.name, SqlMonikerType.TABLE))
        }
        parent.findAliases(result)
    }

    @SuppressWarnings("deprecation")
    @Override
    fun findQualifyingTableName(columnName: String?, ctx: SqlNode?): Pair<String, SqlValidatorNamespace> {
        val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
        val map: Map<String, ScopeChild> = findQualifyingTableNames(columnName, ctx, nameMatcher)
        return when (map.size()) {
            0 -> throw validator.newValidationError(
                ctx,
                RESOURCE.columnNotFound(columnName)
            )
            1 -> {
                val entry: Map.Entry<String, ScopeChild> = map.entrySet().iterator().next()
                Pair.of(entry.getKey(), entry.getValue().namespace)
            }
            else -> throw validator.newValidationError(
                ctx,
                RESOURCE.columnAmbiguous(columnName)
            )
        }
    }

    @Override
    fun findQualifyingTableNames(
        columnName: String?, ctx: SqlNode?,
        nameMatcher: SqlNameMatcher
    ): Map<String, ScopeChild> {
        val map: Map<String, ScopeChild> = HashMap()
        for (child in children) {
            val resolved = ResolvedImpl()
            resolve(
                ImmutableList.of(child.name, columnName), nameMatcher, true,
                resolved
            )
            if (resolved.count() > 0) {
                map.put(child.name, child)
            }
        }
        return when (map.size()) {
            0 -> parent.findQualifyingTableNames(columnName, ctx, nameMatcher)
            else -> map
        }
    }

    @Override
    fun resolve(
        names: List<String>, nameMatcher: SqlNameMatcher,
        deep: Boolean, resolved: Resolved
    ) {
        // First resolve by looking through the child namespaces.
        val child0: ScopeChild? = findChild(names, nameMatcher)
        if (child0 != null) {
            val path: Step = Path.EMPTY.plus(
                child0.namespace.getRowType(), child0.ordinal,
                child0.name, StructKind.FULLY_QUALIFIED
            )
            resolved.found(child0.namespace, child0.nullable, this, path, null)
            return
        }

        // Recursively look deeper into the record-valued fields of the namespace,
        // if it allows skipping fields.
        if (deep) {
            for (child in children) {
                // If identifier starts with table alias, remove the alias.
                val names2 = if (nameMatcher.matches(child.name, names[0])) names.subList(1, names.size()) else names
                resolveInNamespace(
                    child.namespace, child.nullable, names2, nameMatcher,
                    Path.EMPTY, resolved
                )
            }
            if (resolved.count() > 0) {
                return
            }
        }

        // Then call the base class method, which will delegate to the
        // parent scope.
        super.resolve(names, nameMatcher, deep, resolved)
    }

    @Override
    @Nullable
    fun resolveColumn(columnName: String?, ctx: SqlNode?): RelDataType? {
        val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
        var found = 0
        var type: RelDataType? = null
        for (child in children) {
            val childNs: SqlValidatorNamespace = child.namespace
            val childRowType: RelDataType = childNs.getRowType()
            val field: RelDataTypeField = nameMatcher.field(childRowType, columnName)
            if (field != null) {
                found++
                type = field.getType()
            }
        }
        return when (found) {
            0 -> null
            1 -> type
            else -> throw validator.newValidationError(
                ctx,
                RESOURCE.columnAmbiguous(columnName)
            )
        }
    }

    companion object {
        @Nullable
        private fun getQualifiedName(@Nullable table: SqlValidatorTable?): List<String>? {
            return if (table == null) null else table.getQualifiedName()
        }
    }
}
