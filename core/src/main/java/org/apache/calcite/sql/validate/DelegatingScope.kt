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

import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.type.DynamicRecordType
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rel.type.StructKind
import org.apache.calcite.schema.CustomColumnResolvingTable
import org.apache.calcite.schema.Table
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.Comparator
import java.util.List
import java.util.Map
import org.apache.calcite.util.Static.RESOURCE
import java.util.Objects.requireNonNull

/**
 * A scope which delegates all requests to its parent scope. Use this as a base
 * class for defining nested scopes.
 */
abstract class DelegatingScope internal constructor(parent: SqlValidatorScope?) : SqlValidatorScope {
    //~ Instance fields --------------------------------------------------------
    /**
     * Parent scope. This is where to look next to resolve an identifier; it is
     * not always the parent object in the parse tree.
     *
     *
     * This is never null: at the top of the tree, it is an
     * [EmptyScope].
     */
    protected val parent: SqlValidatorScope?
    protected val validator: SqlValidatorImpl
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `DelegatingScope`.
     *
     * @param parent Parent scope
     */
    init {
        assert(parent != null)
        validator = parent.getValidator() as SqlValidatorImpl
        this.parent = parent
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun addChild(
        ns: SqlValidatorNamespace?, alias: String?,
        nullable: Boolean
    ) {
        // By default, you cannot add to a scope. Derived classes can
        // override.
        throw UnsupportedOperationException()
    }

    @Override
    fun resolve(
        names: List<String?>?, nameMatcher: SqlNameMatcher?,
        deep: Boolean, resolved: Resolved?
    ) {
        parent.resolve(names, nameMatcher, deep, resolved)
    }

    /** If a record type allows implicit references to fields, recursively looks
     * into the fields. Otherwise returns immediately.  */
    fun resolveInNamespace(
        ns: SqlValidatorNamespace, nullable: Boolean,
        names: List<String?>, nameMatcher: SqlNameMatcher, path: Path,
        resolved: Resolved
    ) {
        if (names.isEmpty()) {
            resolved.found(ns, nullable, this, path, null)
            return
        }
        val rowType: RelDataType = ns.getRowType()
        if (rowType.isStruct()) {
            val validatorTable: SqlValidatorTable = ns.getTable()
            if (validatorTable is Prepare.PreparingTable) {
                val t: Table = (validatorTable as Prepare.PreparingTable).unwrap(Table::class.java)
                if (t is CustomColumnResolvingTable) {
                    val entries: List<Pair<RelDataTypeField, List<String>>> =
                        (t as CustomColumnResolvingTable).resolveColumn(
                            rowType, validator.getTypeFactory(), names
                        )
                    for (entry in entries) {
                        val field: RelDataTypeField = entry.getKey()
                        val remainder: List<String> = entry.getValue()
                        val ns2: SqlValidatorNamespace = FieldNamespace(validator, field.getType())
                        val path2: Step = path.plus(
                            rowType, field.getIndex(),
                            field.getName(), StructKind.FULLY_QUALIFIED
                        )
                        resolveInNamespace(
                            ns2, nullable, remainder, nameMatcher, path2,
                            resolved
                        )
                    }
                    return
                }
            }
            val name = names[0]
            val field0: RelDataTypeField = nameMatcher.field(rowType, name)
            if (field0 != null) {
                val ns2: SqlValidatorNamespace = requireNonNull(
                    ns.lookupChild(field0.getName())
                ) { "field " + field0.getName().toString() + " is not found in " + ns }
                val path2: Step = path.plus(
                    rowType, field0.getIndex(),
                    field0.getName(), StructKind.FULLY_QUALIFIED
                )
                resolveInNamespace(
                    ns2, nullable, names.subList(1, names.size()),
                    nameMatcher, path2, resolved
                )
            } else {
                for (field in rowType.getFieldList()) {
                    when (field.getType().getStructKind()) {
                        PEEK_FIELDS, PEEK_FIELDS_DEFAULT, PEEK_FIELDS_NO_EXPAND -> {
                            val path2: Step = path.plus(
                                rowType, field.getIndex(),
                                field.getName(), field.getType().getStructKind()
                            )
                            val ns2: SqlValidatorNamespace = requireNonNull(
                                ns.lookupChild(field.getName())
                            ) { "field " + field.getName().toString() + " is not found in " + ns }
                            resolveInNamespace(
                                ns2, nullable, names, nameMatcher, path2,
                                resolved
                            )
                        }
                        else -> {}
                    }
                }
            }
        }
    }

    protected fun addColumnNames(
        ns: SqlValidatorNamespace,
        colNames: List<SqlMoniker?>
    ) {
        val rowType: RelDataType
        rowType = try {
            ns.getRowType()
        } catch (e: Error) {
            // namespace is not good - bail out.
            return
        }
        for (field in rowType.getFieldList()) {
            colNames.add(
                SqlMonikerImpl(
                    field.getName(),
                    SqlMonikerType.COLUMN
                )
            )
        }
    }

    @Override
    fun findAllColumnNames(result: List<SqlMoniker?>?) {
        parent.findAllColumnNames(result)
    }

    @Override
    fun findAliases(result: Collection<SqlMoniker?>?) {
        parent.findAliases(result)
    }

    @SuppressWarnings("deprecation")
    @Override
    fun findQualifyingTableName(
        columnName: String?, ctx: SqlNode?
    ): Pair<String, SqlValidatorNamespace> {
        return parent.findQualifyingTableName(columnName, ctx)
    }

    @Override
    fun findQualifyingTableNames(
        columnName: String?,
        ctx: SqlNode?, nameMatcher: SqlNameMatcher?
    ): Map<String, ScopeChild> {
        return parent.findQualifyingTableNames(columnName, ctx, nameMatcher)
    }

    @Override
    @Nullable
    fun resolveColumn(name: String?, ctx: SqlNode?): RelDataType {
        return parent.resolveColumn(name, ctx)
    }

    @Override
    fun nullifyType(node: SqlNode?, type: RelDataType?): RelDataType {
        return parent.nullifyType(node, type)
    }

    @SuppressWarnings("deprecation")
    @Override
    @Nullable
    fun getTableNamespace(names: List<String?>?): SqlValidatorNamespace {
        return parent.getTableNamespace(names)
    }

    @Override
    fun resolveTable(
        names: List<String?>?, nameMatcher: SqlNameMatcher?,
        path: Path?, resolved: Resolved?
    ) {
        parent.resolveTable(names, nameMatcher, path, resolved)
    }

    @Override
    fun getOperandScope(call: SqlCall?): SqlValidatorScope {
        return if (call is SqlSelect) {
            validator.getSelectScope(call as SqlSelect?)
        } else this
    }

    @Override
    fun getValidator(): SqlValidator {
        return validator
    }

    /**
     * Converts an identifier into a fully-qualified identifier. For example,
     * the "empno" in "select empno from emp natural join dept" becomes
     * "emp.empno".
     *
     *
     * If the identifier cannot be resolved, throws. Never returns null.
     */
    @Override
    fun fullyQualify(identifier: SqlIdentifier): SqlQualified {
        var identifier: SqlIdentifier = identifier
        if (identifier.isStar()) {
            return SqlQualified.create(this, 1, null, identifier)
        }
        val previous: SqlIdentifier = identifier
        val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
        var columnName: String
        var tableName: String
        var namespace: SqlValidatorNamespace
        when (identifier.names.size()) {
            1 -> {
                run {
                    columnName = identifier.names.get(0)
                    val map: Map<String, ScopeChild> = findQualifyingTableNames(columnName, identifier, nameMatcher)
                    when (map.size()) {
                        0 -> {
                            if (nameMatcher.isCaseSensitive()) {
                                val liberalMatcher: SqlNameMatcher = SqlNameMatchers.liberal()
                                val map2: Map<String, ScopeChild> =
                                    findQualifyingTableNames(columnName, identifier, liberalMatcher)
                                if (!map2.isEmpty()) {
                                    val list: List<String> = ArrayList()
                                    for (entry in map2.values()) {
                                        val field: RelDataTypeField = liberalMatcher.field(
                                            entry.namespace.getRowType(),
                                            columnName
                                        ) ?: continue
                                        list.add(field.getName())
                                    }
                                    Collections.sort(list)
                                    throw validator.newValidationError(
                                        identifier,
                                        RESOURCE.columnNotFoundDidYouMean(
                                            columnName,
                                            Util.sepList(list, "', '")
                                        )
                                    )
                                }
                            }
                            throw validator.newValidationError(
                                identifier,
                                RESOURCE.columnNotFound(columnName)
                            )
                        }
                        1 -> {
                            tableName = map.keySet().iterator().next()
                            namespace = map[tableName].namespace
                        }
                        else -> throw validator.newValidationError(
                            identifier,
                            RESOURCE.columnAmbiguous(columnName)
                        )
                    }
                    val resolved = ResolvedImpl()
                    resolveInNamespace(
                        namespace, false, identifier.names, nameMatcher,
                        Path.EMPTY, resolved
                    )
                    val field: RelDataTypeField = nameMatcher.field(namespace.getRowType(), columnName)
                    if (field != null) {
                        if (hasAmbiguousField(
                                namespace.getRowType(), field,
                                columnName, nameMatcher
                            )
                        ) {
                            throw validator.newValidationError(
                                identifier,
                                RESOURCE.columnAmbiguous(columnName)
                            )
                        }
                        columnName = field.getName() // use resolved field name
                    }
                    // todo: do implicit collation here
                    val pos: SqlParserPos = identifier.getParserPosition()
                    identifier = SqlIdentifier(
                        ImmutableList.of(tableName, columnName), null,
                        pos, ImmutableList.of(SqlParserPos.ZERO, pos)
                    )
                }
                run {
                    var fromNs: SqlValidatorNamespace? = null
                    var fromPath: Path? = null
                    var fromRowType: RelDataType? = null
                    val resolved = ResolvedImpl()
                    var size: Int = identifier.names.size()
                    var i = size - 1
                    while (i > 0) {
                        val prefix: SqlIdentifier = identifier.getComponent(0, i)
                        resolved.clear()
                        resolve(prefix.names, nameMatcher, false, resolved)
                        if (resolved.count() === 1) {
                            val resolve: Resolve = resolved.only()
                            fromNs = resolve.namespace
                            fromPath = resolve.path
                            fromRowType = resolve.rowType()
                            break
                        }
                        // Look for a table alias that is the wrong case.
                        if (nameMatcher.isCaseSensitive()) {
                            val liberalMatcher: SqlNameMatcher = SqlNameMatchers.liberal()
                            resolved.clear()
                            resolve(prefix.names, liberalMatcher, false, resolved)
                            if (resolved.count() === 1) {
                                val lastStep: Step = Util.last(resolved.only().path.steps())
                                throw validator.newValidationError(
                                    prefix,
                                    RESOURCE.tableNameNotFoundDidYouMean(
                                        prefix.toString(),
                                        lastStep.name
                                    )
                                )
                            }
                        }
                        i--
                    }
                    if (fromNs == null || fromNs is SchemaNamespace) {
                        // Look for a column not qualified by a table alias.
                        columnName = identifier.names.get(0)
                        val map: Map<String, ScopeChild> = findQualifyingTableNames(columnName, identifier, nameMatcher)
                        when (map.size()) {
                            1 -> {
                                val entry: Map.Entry<String, ScopeChild> = map.entrySet().iterator().next()
                                val tableName2: String = map.keySet().iterator().next()
                                fromNs = entry.getValue().namespace
                                fromPath = Path.EMPTY

                                // Adding table name is for RecordType column with StructKind.PEEK_FIELDS or
                                // StructKind.PEEK_FIELDS only. Access to a field in a RecordType column of
                                // other StructKind should always be qualified with table name.
                                val field: RelDataTypeField = nameMatcher.field(fromNs.getRowType(), columnName)
                                if (field != null) {
                                    when (field.getType().getStructKind()) {
                                        PEEK_FIELDS, PEEK_FIELDS_DEFAULT, PEEK_FIELDS_NO_EXPAND -> {
                                            columnName = field.getName() // use resolved field name
                                            resolve(
                                                ImmutableList.of(tableName2), nameMatcher, false,
                                                resolved
                                            )
                                            if (resolved.count() === 1) {
                                                val resolve: Resolve = resolved.only()
                                                fromNs = resolve.namespace
                                                fromPath = resolve.path
                                                fromRowType = resolve.rowType()
                                                identifier = identifier
                                                    .setName(0, columnName)
                                                    .add(0, tableName2, SqlParserPos.ZERO)
                                                ++i
                                                ++size
                                            }
                                        }
                                        else -> {
                                            // Throw an error if the table was not found.
                                            // If one or more of the child namespaces allows peeking
                                            // (e.g. if they are Phoenix column families) then we relax the SQL
                                            // standard requirement that record fields are qualified by table alias.
                                            val prefix: SqlIdentifier = identifier.skipLast(1)
                                            throw validator.newValidationError(
                                                prefix,
                                                RESOURCE.tableNameNotFound(prefix.toString())
                                            )
                                        }
                                    }
                                }
                            }
                            else -> {
                                val prefix1: SqlIdentifier = identifier.skipLast(1)
                                throw validator.newValidationError(
                                    prefix1,
                                    RESOURCE.tableNameNotFound(prefix1.toString())
                                )
                            }
                        }
                    }

                    // If a table alias is part of the identifier, make sure that the table
                    // alias uses the same case as it was defined. For example, in
                    //
                    //    SELECT e.empno FROM Emp as E
                    //
                    // change "e.empno" to "E.empno".
                    if (fromNs.getEnclosingNode() != null
                        && this !is MatchRecognizeScope
                    ) {
                        val alias: String = SqlValidatorUtil.getAlias(fromNs.getEnclosingNode(), -1)
                        if (alias != null && i > 0 && !alias.equals(identifier.names.get(i - 1))) {
                            identifier = identifier.setName(i - 1, alias)
                        }
                    }
                    if (requireNonNull(fromPath, "fromPath").stepCount() > 1) {
                        assert(fromRowType != null)
                        for (p in fromPath.steps()) {
                            fromRowType = fromRowType.getFieldList().get(p.i).getType()
                        }
                        ++i
                    }
                    val suffix: SqlIdentifier = identifier.getComponent(i, size)
                    resolved.clear()
                    resolveInNamespace(
                        fromNs, false, suffix.names, nameMatcher, Path.EMPTY,
                        resolved
                    )
                    val path: Path
                    when (resolved.count()) {
                        0 -> {
                            // Maybe the last component was correct, just wrong case
                            if (nameMatcher.isCaseSensitive()) {
                                val liberalMatcher: SqlNameMatcher = SqlNameMatchers.liberal()
                                resolved.clear()
                                resolveInNamespace(
                                    fromNs, false, suffix.names, liberalMatcher,
                                    Path.EMPTY, resolved
                                )
                                if (resolved.count() > 0) {
                                    val k = size - 1
                                    val prefix: SqlIdentifier = identifier.getComponent(0, i)
                                    val suffix3: SqlIdentifier = identifier.getComponent(i, k + 1)
                                    val step: Step = Util.last(resolved.resolves.get(0).path.steps())
                                    throw validator.newValidationError(
                                        suffix3,
                                        RESOURCE.columnNotFoundInTableDidYouMean(
                                            suffix3.toString(),
                                            prefix.toString(), step.name
                                        )
                                    )
                                }
                            }
                            // Find the shortest suffix that also fails. Suppose we cannot resolve
                            // "a.b.c"; we find we cannot resolve "a.b" but can resolve "a". So,
                            // the error will be "Column 'a.b' not found".
                            var k = size - 1
                            while (k > i) {
                                val suffix2: SqlIdentifier = identifier.getComponent(i, k)
                                resolved.clear()
                                resolveInNamespace(
                                    fromNs, false, suffix2.names, nameMatcher,
                                    Path.EMPTY, resolved
                                )
                                if (resolved.count() > 0) {
                                    break
                                }
                                --k
                            }
                            val prefix: SqlIdentifier = identifier.getComponent(0, i)
                            val suffix3: SqlIdentifier = identifier.getComponent(i, k + 1)
                            throw validator.newValidationError(
                                suffix3,
                                RESOURCE.columnNotFoundInTable(suffix3.toString(), prefix.toString())
                            )
                        }
                        1 -> path = resolved.only().path
                        else -> {
                            val c: Comparator<Resolve> = object : Comparator<Resolve?>() {
                                @Override
                                fun compare(o1: Resolve, o2: Resolve): Int {
                                    // Name resolution that uses fewer implicit steps wins.
                                    val c: Int = Integer.compare(worstKind(o1.path), worstKind(o2.path))
                                    return if (c != 0) {
                                        c
                                    } else Integer.compare(o1.path.stepCount(), o2.path.stepCount())
                                    // Shorter path wins
                                }

                                private fun worstKind(path: Path): Int {
                                    var kind = -1
                                    for (step in path.steps()) {
                                        kind = Math.max(kind, step.kind.ordinal())
                                    }
                                    return kind
                                }
                            }
                            resolved.resolves.sort(c)
                            if (c.compare(resolved.resolves.get(0), resolved.resolves.get(1)) === 0) {
                                throw validator.newValidationError(
                                    suffix,
                                    RESOURCE.columnAmbiguous(suffix.toString())
                                )
                            }
                            path = resolved.resolves.get(0).path
                        }
                    }

                    // Normalize case to match definition, make elided fields explicit,
                    // and check that references to dynamic stars ("**") are unambiguous.
                    var k = i
                    for (step in path.steps()) {
                        val name: String = identifier.names.get(k)
                        if (step.i < 0) {
                            throw validator.newValidationError(
                                identifier, RESOURCE.columnNotFound(name)
                            )
                        }
                        val field0: RelDataTypeField = requireNonNull(
                            step.rowType
                        ) { "rowType of step " + step.name }.getFieldList().get(step.i)
                        val fieldName: String = field0.getName()
                        when (step.kind) {
                            PEEK_FIELDS, PEEK_FIELDS_DEFAULT, PEEK_FIELDS_NO_EXPAND -> identifier =
                                identifier.add(k, fieldName, SqlParserPos.ZERO)
                            else -> {
                                if (!fieldName.equals(name)) {
                                    identifier = identifier.setName(k, fieldName)
                                }
                                if (hasAmbiguousField(step.rowType, field0, name, nameMatcher)) {
                                    throw validator.newValidationError(
                                        identifier,
                                        RESOURCE.columnAmbiguous(name)
                                    )
                                }
                            }
                        }
                        ++k
                    }

                    // Multiple name components may have been resolved as one step by
                    // CustomResolvingTable.
                    if (identifier.names.size() > k) {
                        identifier = identifier.getComponent(0, k)
                    }
                    if (i > 1) {
                        // Simplify overqualified identifiers.
                        // For example, schema.emp.deptno becomes emp.deptno.
                        //
                        // It is safe to convert schema.emp or database.schema.emp to emp
                        // because it would not have resolved if the FROM item had an alias. The
                        // following query is invalid:
                        //   SELECT schema.emp.deptno FROM schema.emp AS e
                        identifier = identifier.getComponent(i - 1, identifier.names.size())
                    }
                    if (!previous.equals(identifier)) {
                        validator.setOriginal(identifier, previous)
                    }
                    return SqlQualified.create(this, i, fromNs, identifier)
                }
            }
            else -> {
                var fromNs: SqlValidatorNamespace? = null
                var fromPath: Path? = null
                var fromRowType: RelDataType? = null
                val resolved = ResolvedImpl()
                var size: Int = identifier.names.size()
                var i = size - 1
                while (i > 0) {
                    val prefix: SqlIdentifier = identifier.getComponent(0, i)
                    resolved.clear()
                    resolve(prefix.names, nameMatcher, false, resolved)
                    if (resolved.count() === 1) {
                        val resolve: Resolve = resolved.only()
                        fromNs = resolve.namespace
                        fromPath = resolve.path
                        fromRowType = resolve.rowType()
                        break
                    }
                    if (nameMatcher.isCaseSensitive()) {
                        val liberalMatcher: SqlNameMatcher = SqlNameMatchers.liberal()
                        resolved.clear()
                        resolve(prefix.names, liberalMatcher, false, resolved)
                        if (resolved.count() === 1) {
                            val lastStep: Step = Util.last(resolved.only().path.steps())
                            throw validator.newValidationError(
                                prefix,
                                RESOURCE.tableNameNotFoundDidYouMean(
                                    prefix.toString(),
                                    lastStep.name
                                )
                            )
                        }
                    }
                    i--
                }
                if (fromNs == null || fromNs is SchemaNamespace) {
                    columnName = identifier.names.get(0)
                    val map: Map<String, ScopeChild> = findQualifyingTableNames(columnName, identifier, nameMatcher)
                    when (map.size()) {
                        1 -> {
                            val entry: Map.Entry<String, ScopeChild> = map.entrySet().iterator().next()
                            val tableName2: String = map.keySet().iterator().next()
                            fromNs = entry.getValue().namespace
                            fromPath = Path.EMPTY
                            val field: RelDataTypeField = nameMatcher.field(fromNs.getRowType(), columnName)
                            if (field != null) {
                                when (field.getType().getStructKind()) {
                                    PEEK_FIELDS, PEEK_FIELDS_DEFAULT, PEEK_FIELDS_NO_EXPAND -> {
                                        columnName = field.getName()
                                        resolve(
                                            ImmutableList.of(tableName2), nameMatcher, false,
                                            resolved
                                        )
                                        if (resolved.count() === 1) {
                                            val resolve: Resolve = resolved.only()
                                            fromNs = resolve.namespace
                                            fromPath = resolve.path
                                            fromRowType = resolve.rowType()
                                            identifier = identifier
                                                .setName(0, columnName)
                                                .add(0, tableName2, SqlParserPos.ZERO)
                                            ++i
                                            ++size
                                        }
                                    }
                                    else -> {
                                        val prefix: SqlIdentifier = identifier.skipLast(1)
                                        throw validator.newValidationError(
                                            prefix,
                                            RESOURCE.tableNameNotFound(prefix.toString())
                                        )
                                    }
                                }
                            }
                        }
                        else -> {
                            val prefix1: SqlIdentifier = identifier.skipLast(1)
                            throw validator.newValidationError(
                                prefix1,
                                RESOURCE.tableNameNotFound(prefix1.toString())
                            )
                        }
                    }
                }
                if (fromNs.getEnclosingNode() != null
                    && this !is MatchRecognizeScope
                ) {
                    val alias: String = SqlValidatorUtil.getAlias(fromNs.getEnclosingNode(), -1)
                    if (alias != null && i > 0 && !alias.equals(identifier.names.get(i - 1))) {
                        identifier = identifier.setName(i - 1, alias)
                    }
                }
                if (requireNonNull(fromPath, "fromPath").stepCount() > 1) {
                    assert(fromRowType != null)
                    for (p in fromPath.steps()) {
                        fromRowType = fromRowType.getFieldList().get(p.i).getType()
                    }
                    ++i
                }
                val suffix: SqlIdentifier = identifier.getComponent(i, size)
                resolved.clear()
                resolveInNamespace(
                    fromNs, false, suffix.names, nameMatcher, Path.EMPTY,
                    resolved
                )
                val path: Path
                when (resolved.count()) {
                    0 -> {
                        if (nameMatcher.isCaseSensitive()) {
                            val liberalMatcher: SqlNameMatcher = SqlNameMatchers.liberal()
                            resolved.clear()
                            resolveInNamespace(
                                fromNs, false, suffix.names, liberalMatcher,
                                Path.EMPTY, resolved
                            )
                            if (resolved.count() > 0) {
                                val k = size - 1
                                val prefix: SqlIdentifier = identifier.getComponent(0, i)
                                val suffix3: SqlIdentifier = identifier.getComponent(i, k + 1)
                                val step: Step = Util.last(resolved.resolves.get(0).path.steps())
                                throw validator.newValidationError(
                                    suffix3,
                                    RESOURCE.columnNotFoundInTableDidYouMean(
                                        suffix3.toString(),
                                        prefix.toString(), step.name
                                    )
                                )
                            }
                        }
                        var k = size - 1
                        while (k > i) {
                            val suffix2: SqlIdentifier = identifier.getComponent(i, k)
                            resolved.clear()
                            resolveInNamespace(
                                fromNs, false, suffix2.names, nameMatcher,
                                Path.EMPTY, resolved
                            )
                            if (resolved.count() > 0) {
                                break
                            }
                            --k
                        }
                        val prefix: SqlIdentifier = identifier.getComponent(0, i)
                        val suffix3: SqlIdentifier = identifier.getComponent(i, k + 1)
                        throw validator.newValidationError(
                            suffix3,
                            RESOURCE.columnNotFoundInTable(suffix3.toString(), prefix.toString())
                        )
                    }
                    1 -> path = resolved.only().path
                    else -> {
                        val c: Comparator<Resolve> = object : Comparator<Resolve?>() {
                            @Override
                            fun compare(o1: Resolve, o2: Resolve): Int {
                                val c: Int = Integer.compare(worstKind(o1.path), worstKind(o2.path))
                                return if (c != 0) {
                                    c
                                } else Integer.compare(o1.path.stepCount(), o2.path.stepCount())
                            }

                            private fun worstKind(path: Path): Int {
                                var kind = -1
                                for (step in path.steps()) {
                                    kind = Math.max(kind, step.kind.ordinal())
                                }
                                return kind
                            }
                        }
                        resolved.resolves.sort(c)
                        if (c.compare(resolved.resolves.get(0), resolved.resolves.get(1)) === 0) {
                            throw validator.newValidationError(
                                suffix,
                                RESOURCE.columnAmbiguous(suffix.toString())
                            )
                        }
                        path = resolved.resolves.get(0).path
                    }
                }
                var k = i
                for (step in path.steps()) {
                    val name: String = identifier.names.get(k)
                    if (step.i < 0) {
                        throw validator.newValidationError(
                            identifier, RESOURCE.columnNotFound(name)
                        )
                    }
                    val field0: RelDataTypeField = requireNonNull(
                        step.rowType
                    ) { "rowType of step " + step.name }.getFieldList().get(step.i)
                    val fieldName: String = field0.getName()
                    when (step.kind) {
                        PEEK_FIELDS, PEEK_FIELDS_DEFAULT, PEEK_FIELDS_NO_EXPAND -> identifier =
                            identifier.add(k, fieldName, SqlParserPos.ZERO)
                        else -> {
                            if (!fieldName.equals(name)) {
                                identifier = identifier.setName(k, fieldName)
                            }
                            if (hasAmbiguousField(step.rowType, field0, name, nameMatcher)) {
                                throw validator.newValidationError(
                                    identifier,
                                    RESOURCE.columnAmbiguous(name)
                                )
                            }
                        }
                    }
                    ++k
                }
                if (identifier.names.size() > k) {
                    identifier = identifier.getComponent(0, k)
                }
                if (i > 1) {
                    identifier = identifier.getComponent(i - 1, identifier.names.size())
                }
                if (!previous.equals(identifier)) {
                    validator.setOriginal(identifier, previous)
                }
                return SqlQualified.create(this, i, fromNs, identifier)
            }
        }
    }

    @Override
    fun validateExpr(expr: SqlNode?) {
        // Do not delegate to parent. An expression valid in this scope may not
        // be valid in the parent scope.
    }

    @Override
    @Nullable
    fun lookupWindow(name: String?): SqlWindow {
        return parent.lookupWindow(name)
    }

    @Override
    fun getMonotonicity(expr: SqlNode?): SqlMonotonicity {
        return parent.getMonotonicity(expr)
    }

    @get:Nullable
    @get:Override
    val orderList: SqlNodeList
        get() = parent.getOrderList()

    /**
     * Returns the parent scope of this `DelegatingScope`.
     */
    fun getParent(): SqlValidatorScope? {
        return parent
    }

    companion object {
        /** Returns whether `rowType` contains more than one star column or
         * fields with the same name, which implies ambiguous column.  */
        private fun hasAmbiguousField(
            rowType: RelDataType,
            field: RelDataTypeField?, columnName: String?, nameMatcher: SqlNameMatcher
        ): Boolean {
            if (field.isDynamicStar()
                && !DynamicRecordType.isDynamicStarColName(columnName)
            ) {
                var count = 0
                for (possibleStar in rowType.getFieldList()) {
                    if (possibleStar.isDynamicStar()) {
                        if (++count > 1) {
                            return true
                        }
                    }
                }
            } else { // check if there are fields with the same name
                var count = 0
                for (f in rowType.getFieldList()) {
                    if (Util.matches(nameMatcher.isCaseSensitive(), f.getName(), columnName)) {
                        count++
                    }
                }
                if (count > 1) {
                    return true
                }
            }
            return false
        }
    }
}
