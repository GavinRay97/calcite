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
 * Namespace whose contents are defined by the type of an
 * [identifier][org.apache.calcite.sql.SqlIdentifier].
 */
class IdentifierNamespace internal constructor(
    validator: SqlValidatorImpl?, id: SqlIdentifier,
    @Nullable extendList: SqlNodeList?, @Nullable enclosingNode: SqlNode?,
    parentScope: SqlValidatorScope?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val id: SqlIdentifier
    private val parentScope: SqlValidatorScope

    @Nullable
    val extendList: SqlNodeList?

    /**
     * The underlying namespace. Often a [TableNamespace].
     * Set on validate.
     */
    @MonotonicNonNull
    private var resolvedNamespace: SqlValidatorNamespace? = null

    /**
     * List of monotonic expressions. Set on validate.
     */
    @Nullable
    private var monotonicExprs: List<Pair<SqlNode, SqlMonotonicity>>? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an IdentifierNamespace.
     *
     * @param validator     Validator
     * @param id            Identifier node (or "identifier EXTEND column-list")
     * @param extendList    Extension columns, or null
     * @param enclosingNode Enclosing node
     * @param parentScope   Parent scope which this namespace turns to in order to
     */
    init {
        this.id = id
        this.extendList = extendList
        this.parentScope = Objects.requireNonNull(parentScope, "parentScope")
    }

    internal constructor(
        validator: SqlValidatorImpl?, node: SqlNode,
        @Nullable enclosingNode: SqlNode?, parentScope: SqlValidatorScope?
    ) : this(
        validator, split(node).left, split(node).right, enclosingNode,
        parentScope
    ) {
    }

    private fun resolveImpl(id: SqlIdentifier): SqlValidatorNamespace {
        val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
        val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
        val names: List<String> = SqlIdentifier.toStar(id.names)
        try {
            parentScope.resolveTable(
                names, nameMatcher,
                SqlValidatorScope.Path.EMPTY, resolved
            )
        } catch (e: CyclicDefinitionException) {
            if (e.depth === 1) {
                throw validator.newValidationError(
                    id,
                    RESOURCE.cyclicDefinition(
                        id.toString(),
                        SqlIdentifier.getString(e.path)
                    )
                )
            } else {
                throw CyclicDefinitionException(e.depth - 1, e.path)
            }
        }
        var previousResolve: Resolve? = null
        if (resolved.count() === 1) {
            previousResolve = resolved.only()
            val resolve: Resolve? = previousResolve
            if (resolve.remainingNames.isEmpty()) {
                return resolve.namespace
            }
            // If we're not case sensitive, give an error.
            // If we're case sensitive, we'll shortly try again and give an error
            // then.
            if (!nameMatcher.isCaseSensitive()) {
                throw validator.newValidationError(
                    id,
                    RESOURCE.objectNotFoundWithin(
                        resolve.remainingNames.get(0),
                        SqlIdentifier.getString(resolve.path.stepNames())
                    )
                )
            }
        }

        // Failed to match.  If we're matching case-sensitively, try a more
        // lenient match. If we find something we can offer a helpful hint.
        if (nameMatcher.isCaseSensitive()) {
            val liberalMatcher: SqlNameMatcher = SqlNameMatchers.liberal()
            resolved.clear()
            parentScope.resolveTable(
                names, liberalMatcher,
                SqlValidatorScope.Path.EMPTY, resolved
            )
            if (resolved.count() === 1) {
                val resolve: Resolve = resolved.only()
                if (resolve.remainingNames.isEmpty()
                    || previousResolve == null
                ) {
                    // We didn't match it case-sensitive, so they must have had the
                    // right identifier, wrong case.
                    //
                    // If previousResolve is null, we matched nothing case-sensitive and
                    // everything case-insensitive, so the mismatch must have been at
                    // position 0.
                    val i = if (previousResolve == null) 0 else previousResolve.path.stepCount()
                    val offset: Int = resolve.path.stepCount()
                    +resolve.remainingNames.size() - names.size()
                    val prefix: List<String> = resolve.path.stepNames().subList(0, offset + i)
                    val next: String = resolve.path.stepNames().get(i + offset)
                    if (prefix.isEmpty()) {
                        throw validator.newValidationError(
                            id,
                            RESOURCE.objectNotFoundDidYouMean(names[i], next)
                        )
                    } else {
                        throw validator.newValidationError(
                            id,
                            RESOURCE.objectNotFoundWithinDidYouMean(
                                names[i],
                                SqlIdentifier.getString(prefix), next
                            )
                        )
                    }
                } else {
                    throw validator.newValidationError(
                        id,
                        RESOURCE.objectNotFoundWithin(
                            resolve.remainingNames.get(0),
                            SqlIdentifier.getString(resolve.path.stepNames())
                        )
                    )
                }
            }
        }
        throw validator.newValidationError(
            id,
            RESOURCE.objectNotFound(id.getComponent(0).toString())
        )
    }

    @Override
    fun validateImpl(targetRowType: RelDataType?): RelDataType {
        resolvedNamespace = resolveImpl(id)
        if (resolvedNamespace is TableNamespace) {
            val table: SqlValidatorTable = (resolvedNamespace as TableNamespace?).getTable()
            if (validator.config().identifierExpansion()) {
                // TODO:  expand qualifiers for column references also
                val qualifiedNames: List<String> = table.getQualifiedName()
                if (qualifiedNames != null) {
                    // Assign positions to the components of the fully-qualified
                    // identifier, as best we can. We assume that qualification
                    // adds names to the front, e.g. FOO.BAR becomes BAZ.FOO.BAR.
                    val poses: List<SqlParserPos> = ArrayList(
                        Collections.nCopies(
                            qualifiedNames.size(), id.getParserPosition()
                        )
                    )
                    val offset: Int = qualifiedNames.size() - id.names.size()

                    // Test offset in case catalog supports fewer qualifiers than catalog
                    // reader.
                    if (offset >= 0) {
                        for (i in 0 until id.names.size()) {
                            poses.set(i + offset, id.getComponentParserPosition(i))
                        }
                    }
                    id.setNames(qualifiedNames, poses)
                }
            }
        }
        var rowType: RelDataType = resolvedNamespace.getRowType()
        if (extendList != null) {
            if (resolvedNamespace !is TableNamespace) {
                throw RuntimeException("cannot convert")
            }
            resolvedNamespace = (resolvedNamespace as TableNamespace).extend(extendList)
            rowType = resolvedNamespace.getRowType()
        }

        // Build a list of monotonic expressions.
        val builder: ImmutableList.Builder<Pair<SqlNode, SqlMonotonicity>> = ImmutableList.builder()
        val fields: List<RelDataTypeField> = rowType.getFieldList()
        for (field in fields) {
            val fieldName: String = field.getName()
            val monotonicity: SqlMonotonicity = resolvedNamespace.getMonotonicity(fieldName)
            if (monotonicity != null && monotonicity !== SqlMonotonicity.NOT_MONOTONIC) {
                builder.add(
                    Pair.of(
                        SqlIdentifier(fieldName, SqlParserPos.ZERO) as SqlNode?,
                        monotonicity
                    )
                )
            }
        }
        monotonicExprs = builder.build()

        // Validation successful.
        return rowType
    }

    fun getId(): SqlIdentifier {
        return id
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = id

    @Override
    fun resolve(): SqlValidatorNamespace {
        assert(resolvedNamespace != null) { "must call validate first" }
        return resolvedNamespace.resolve()
    }

    @get:Nullable
    @get:Override
    val table: SqlValidatorTable?
        get() = if (resolvedNamespace == null) null else resolve().getTable()

    @Override
    fun getMonotonicExprs(): List<Pair<SqlNode, SqlMonotonicity>> {
        val monotonicExprs: List<Pair<SqlNode, SqlMonotonicity>>? = monotonicExprs
        return monotonicExprs ?: ImmutableList.of()
    }

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        val table: SqlValidatorTable = table ?: return SqlMonotonicity.NOT_MONOTONIC
        return table.getMonotonicity(columnName)
    }

    @Override
    fun supportsModality(modality: SqlModality): Boolean {
        val table: SqlValidatorTable = table ?: return modality === SqlModality.RELATION
        return table.supportsModality(modality)
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        protected fun split(node: SqlNode): Pair<SqlIdentifier, SqlNodeList> {
            return when (node.getKind()) {
                EXTEND -> {
                    val call: SqlCall = node as SqlCall
                    val operand0: SqlNode = call.operand(0)
                    val identifier: SqlIdentifier =
                        if (operand0.getKind() === SqlKind.TABLE_REF) (operand0 as SqlCall).operand(0) else operand0 as SqlIdentifier
                    Pair.of(identifier, call.operand(1))
                }
                TABLE_REF -> {
                    val tableRef: SqlCall = node as SqlCall
                    Pair.of(tableRef.operand(0), null)
                }
                else -> Pair.of(node as SqlIdentifier, null)
            }
        }
    }
}
