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

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.StructKind
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.Wrapper
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDynamicParam
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import java.util.ArrayList
import java.util.Collection
import java.util.List
import java.util.Map
import org.apache.calcite.util.Static.RESOURCE

/**
 * Deviant implementation of [SqlValidatorScope] for the top of the scope
 * stack.
 *
 *
 * It is convenient, because we never need to check whether a scope's parent
 * is null. (This scope knows not to ask about its parents, just like Adam.)
 */
internal class EmptyScope(validator: SqlValidatorImpl) : SqlValidatorScope {
    //~ Instance fields --------------------------------------------------------
    protected val validator: SqlValidatorImpl

    //~ Constructors -----------------------------------------------------------
    init {
        this.validator = validator
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getValidator(): SqlValidator {
        return validator
    }

    @Override
    fun fullyQualify(identifier: SqlIdentifier?): SqlQualified {
        return SqlQualified.create(this, 1, null, identifier)
    }

    @get:Override
    val node: SqlNode
        get() {
            throw UnsupportedOperationException()
        }

    @Override
    fun resolve(
        names: List<String?>?, nameMatcher: SqlNameMatcher?,
        deep: Boolean, resolved: Resolved?
    ) {
    }

    @SuppressWarnings("deprecation")
    @Override
    @Nullable
    fun getTableNamespace(names: List<String?>?): SqlValidatorNamespace? {
        val table: SqlValidatorTable = validator.catalogReader.getTable(names)
        return if (table != null) TableNamespace(validator, table) else null
    }

    @Override
    fun resolveTable(
        names: List<String>, nameMatcher: SqlNameMatcher,
        path: Path, resolved: Resolved
    ) {
        val imperfectResolves: List<Resolve> = ArrayList()
        val resolves: List<Resolve> = (resolved as ResolvedImpl).resolves

        // Look in the default schema, then default catalog, then root schema.
        for (schemaPath in validator.catalogReader.getSchemaPaths()) {
            resolve_(
                validator.catalogReader.getRootSchema(), names, schemaPath,
                nameMatcher, path, resolved
            )
            for (resolve in resolves) {
                if (resolve.remainingNames.isEmpty()) {
                    // There is a full match. Return it as the only match.
                    (resolved as ResolvedImpl).clear()
                    resolves.add(resolve)
                    return
                }
            }
            imperfectResolves.addAll(resolves)
        }
        // If there were no matches in the last round, restore those found in
        // previous rounds
        if (resolves.isEmpty()) {
            resolves.addAll(imperfectResolves)
        }
    }

    private fun resolve_(
        rootSchema: CalciteSchema, names: List<String>,
        schemaNames: List<String>, nameMatcher: SqlNameMatcher, path: Path,
        resolved: Resolved
    ) {
        var path: Path = path
        val concat: List<String> = ImmutableList.< String > builder < String ? > ()
            .addAll(schemaNames).addAll(names).build()
        var schema: CalciteSchema? = rootSchema
        var namespace: SqlValidatorNamespace? = null
        var remainingNames: List<String?> = concat
        for (schemaName in concat) {
            if (schema === rootSchema
                && nameMatcher.matches(schemaName, schema.name)
            ) {
                remainingNames = Util.skip(remainingNames)
                continue
            }
            val subSchema: CalciteSchema = schema.getSubSchema(schemaName, nameMatcher.isCaseSensitive())
            if (subSchema != null) {
                path = path.plus(null, -1, subSchema.name, StructKind.NONE)
                remainingNames = Util.skip(remainingNames)
                schema = subSchema
                namespace = SchemaNamespace(
                    validator,
                    ImmutableList.copyOf(path.stepNames())
                )
                continue
            }
            var entry: CalciteSchema.TableEntry = schema.getTable(schemaName, nameMatcher.isCaseSensitive())
            if (entry == null) {
                entry = schema.getTableBasedOnNullaryFunction(
                    schemaName,
                    nameMatcher.isCaseSensitive()
                )
            }
            if (entry != null) {
                path = path.plus(null, -1, entry.name, StructKind.NONE)
                remainingNames = Util.skip(remainingNames)
                val table: Table = entry.getTable()
                var table2: SqlValidatorTable? = null
                if (table is Wrapper) {
                    table2 = (table as Wrapper).unwrap(Prepare.PreparingTable::class.java)
                }
                if (table2 == null) {
                    val relOptSchema: RelOptSchema = validator.catalogReader.unwrap(RelOptSchema::class.java)
                    val rowType: RelDataType = table.getRowType(validator.typeFactory)
                    table2 = RelOptTableImpl.create(relOptSchema, rowType, entry, null)
                }
                namespace = TableNamespace(validator, table2)
                resolved.found(namespace, false, null, path, remainingNames)
                return
            }
            // neither sub-schema nor table
            if (namespace != null
                && !remainingNames.equals(names)
            ) {
                resolved.found(namespace, false, null, path, remainingNames)
            }
            return
        }
    }

    @Override
    fun nullifyType(node: SqlNode?, type: RelDataType): RelDataType {
        return type
    }

    @Override
    fun findAllColumnNames(result: List<SqlMoniker?>?) {
    }

    fun findAllTableNames(result: List<SqlMoniker?>?) {}
    @Override
    fun findAliases(result: Collection<SqlMoniker?>?) {
    }

    @Override
    @Nullable
    fun resolveColumn(name: String?, ctx: SqlNode?): RelDataType? {
        return null
    }

    @Override
    fun getOperandScope(call: SqlCall?): SqlValidatorScope {
        return this
    }

    @Override
    fun validateExpr(expr: SqlNode?) {
        // valid
    }

    @SuppressWarnings("deprecation")
    @Override
    fun findQualifyingTableName(
        columnName: String?, ctx: SqlNode?
    ): Pair<String, SqlValidatorNamespace> {
        throw validator.newValidationError(
            ctx,
            RESOURCE.columnNotFound(columnName)
        )
    }

    @Override
    fun findQualifyingTableNames(
        columnName: String?,
        ctx: SqlNode?, nameMatcher: SqlNameMatcher?
    ): Map<String, ScopeChild> {
        return ImmutableMap.of()
    }

    @Override
    fun addChild(
        ns: SqlValidatorNamespace?, alias: String?,
        nullable: Boolean
    ) {
        // cannot add to the empty scope
        throw UnsupportedOperationException()
    }

    @Override
    @Nullable
    fun lookupWindow(name: String?): SqlWindow? {
        // No windows defined in this scope.
        return null
    }

    @Override
    fun getMonotonicity(expr: SqlNode?): SqlMonotonicity {
        return if (expr is SqlLiteral
            || expr is SqlDynamicParam
            || expr is SqlDataTypeSpec
        ) SqlMonotonicity.CONSTANT else SqlMonotonicity.NOT_MONOTONIC
    }

    // scope is not ordered
    @get:Nullable
    @get:Override
    val orderList: SqlNodeList?
        get() =// scope is not ordered
            null
}
