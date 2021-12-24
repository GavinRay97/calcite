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

import org.apache.calcite.linq4j.Ord

/**
 * Default implementation of [SqlValidator].
 */
class SqlValidatorImpl(
    opTab: SqlOperatorTable?,
    catalogReader: SqlValidatorCatalogReader,
    typeFactory: RelDataTypeFactory,
    config: Config
) : SqlValidatorWithHints {
    //~ Instance fields --------------------------------------------------------
    private val opTab: SqlOperatorTable
    override val catalogReader: SqlValidatorCatalogReader

    /**
     * Maps [SqlParserPos] strings to the [SqlIdentifier] identifier
     * objects at these positions.
     */
    protected val idPositions: Map<String, IdInfo> = HashMap()

    /**
     * Maps [query node][SqlNode] objects to the [SqlValidatorScope]
     * scope created from them.
     */
    protected val scopes: IdentityHashMap<SqlNode, SqlValidatorScope> = IdentityHashMap()

    /**
     * Maps a [SqlSelect] and a clause to the scope used by that clause.
     */
    private val clauseScopes: Map<IdPair<SqlSelect, Clause>, SqlValidatorScope> = HashMap()

    /**
     * The name-resolution scope of a LATERAL TABLE clause.
     */
    @Nullable
    private var tableScope: TableScope? = null

    /**
     * Maps a [node][SqlNode] to the
     * [namespace][SqlValidatorNamespace] which describes what columns they
     * contain.
     */
    protected val namespaces: IdentityHashMap<SqlNode, SqlValidatorNamespace> = IdentityHashMap()

    /**
     * Set of select expressions used as cursor definitions. In standard SQL,
     * only the top-level SELECT is a cursor; Calcite extends this with
     * cursors as inputs to table functions.
     */
    private val cursorSet: Set<SqlNode> = Sets.newIdentityHashSet()

    /**
     * Stack of objects that maintain information about function calls. A stack
     * is needed to handle nested function calls. The function call currently
     * being validated is at the top of the stack.
     */
    protected val functionCallStack: Deque<FunctionParamInfo> = ArrayDeque()
    private var nextGeneratedId = 0
    protected override val typeFactory: RelDataTypeFactory

    /** The type of dynamic parameters until a type is imposed on them.  */
    protected override val unknownType: RelDataType
    private val booleanType: RelDataType

    /**
     * Map of derived RelDataType for each node. This is an IdentityHashMap
     * since in some cases (such as null literals) we need to discriminate by
     * instance.
     */
    private val nodeToTypeMap: IdentityHashMap<SqlNode, RelDataType> = IdentityHashMap()

    /** Provides the data for [.getValidatedOperandTypes].  */
    val callToOperandTypesMap: IdentityHashMap<SqlCall, List<RelDataType>> = IdentityHashMap()
    private val aggFinder: AggFinder
    private val aggOrOverFinder: AggFinder
    private val aggOrOverOrGroupFinder: AggFinder
    private val groupFinder: AggFinder
    private val overFinder: AggFinder
    private var config: Config
    private val originalExprs: Map<SqlNode?, SqlNode> = HashMap()

    @Nullable
    private var top: SqlNode? = null

    // TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
    // if it's OK to expand the signature of that method.
    private var validatingSqlMerge = false
    private var inWindow // Allow nested aggregates
            = false
    val validationErrorFunction: ValidationErrorFunction = ValidationErrorFunction()

    // TypeCoercion instance used for implicit type coercion.
    private override val typeCoercion: TypeCoercion
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param config        Config
     */
    init {
        this.opTab = requireNonNull(opTab, "opTab")
        this.catalogReader = requireNonNull(catalogReader, "catalogReader")
        this.typeFactory = requireNonNull(typeFactory, "typeFactory")
        this.config = requireNonNull(config, "config")
        unknownType = typeFactory.createUnknownType()
        booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN)
        val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
        aggFinder = AggFinder(opTab, false, true, false, null, nameMatcher)
        aggOrOverFinder = AggFinder(opTab, true, true, false, null, nameMatcher)
        overFinder = AggFinder(
            opTab, true, false, false, aggOrOverFinder,
            nameMatcher
        )
        groupFinder = AggFinder(opTab, false, false, true, null, nameMatcher)
        aggOrOverOrGroupFinder = AggFinder(
            opTab, true, true, true, null,
            nameMatcher
        )
        @SuppressWarnings("argument.type.incompatible") val typeCoercion: TypeCoercion =
            config.typeCoercionFactory().create(typeFactory, this)
        this.typeCoercion = typeCoercion
        if (config.typeCoercionRules() != null) {
            SqlTypeCoercionRule.THREAD_PROVIDERS.set(config.typeCoercionRules())
        }
    }

    //~ Methods ----------------------------------------------------------------
    val conformance: org.apache.calcite.sql.validate.SqlConformance
        get() = config.sqlConformance()

    @Pure
    @Override
    fun getCatalogReader(): SqlValidatorCatalogReader {
        return catalogReader
    }

    @get:Override
    @get:Pure
    override val operatorTable: SqlOperatorTable
        get() = opTab

    @Pure
    @Override
    fun getTypeFactory(): RelDataTypeFactory {
        return typeFactory
    }

    @Override
    fun getUnknownType(): RelDataType {
        return unknownType
    }

    @Override
    override fun expandStar(
        selectList: SqlNodeList,
        select: SqlSelect,
        includeSystemVars: Boolean
    ): SqlNodeList {
        val list: List<SqlNode> = ArrayList()
        val types: List<Map.Entry<String, RelDataType>> = ArrayList()
        for (i in 0 until selectList.size()) {
            val selectItem: SqlNode = selectList.get(i)
            val originalType: RelDataType? = getValidatedNodeTypeIfKnown(selectItem)
            expandSelectItem(
                selectItem,
                select,
                Util.first(originalType, unknownType),
                list,
                catalogReader.nameMatcher().createSet(),
                types,
                includeSystemVars
            )
        }
        getRawSelectScopeNonNull(select).setExpandedSelectList(list)
        return SqlNodeList(list, SqlParserPos.ZERO)
    }

    // implement SqlValidator
    @Override
    override fun declareCursor(select: SqlSelect?, parentScope: SqlValidatorScope?) {
        cursorSet.add(select)

        // add the cursor to a map that maps the cursor to its select based on
        // the position of the cursor relative to other cursors in that call
        val funcParamInfo: FunctionParamInfo = requireNonNull(functionCallStack.peek(), "functionCall")
        val cursorMap: Map<Integer, SqlSelect> = funcParamInfo.cursorPosToSelectMap
        val numCursors: Int = cursorMap.size()
        cursorMap.put(numCursors, select)

        // create a namespace associated with the result of the select
        // that is the argument to the cursor constructor; register it
        // with a scope corresponding to the cursor
        val cursorScope = SelectScope(parentScope, null, select)
        clauseScopes.put(IdPair.of(select, Clause.CURSOR), cursorScope)
        val selectNs: SelectNamespace = createSelectNamespace(select, select)
        val alias = deriveAlias(select, nextGeneratedId++)
        registerNamespace(cursorScope, alias, selectNs, false)
    }

    // implement SqlValidator
    @Override
    override fun pushFunctionCall() {
        val funcInfo = FunctionParamInfo()
        functionCallStack.push(funcInfo)
    }

    // implement SqlValidator
    @Override
    override fun popFunctionCall() {
        functionCallStack.pop()
    }

    // implement SqlValidator
    @Override
    @Nullable
    fun getParentCursor(columnListParamName: String): String? {
        val funcParamInfo: FunctionParamInfo = requireNonNull(functionCallStack.peek(), "functionCall")
        val parentCursorMap = funcParamInfo.columnListParamToParentCursorMap
        return parentCursorMap[columnListParamName]
    }

    /**
     * If `selectItem` is "*" or "TABLE.*", expands it and returns
     * true; otherwise writes the unexpanded item.
     *
     * @param selectItem        Select-list item
     * @param select            Containing select clause
     * @param selectItems       List that expanded items are written to
     * @param aliases           Set of aliases
     * @param fields            List of field names and types, in alias order
     * @param includeSystemVars If true include system vars in lists
     * @return Whether the node was expanded
     */
    private fun expandSelectItem(
        selectItem: SqlNode,
        select: SqlSelect,
        targetType: RelDataType,
        selectItems: List<SqlNode>,
        aliases: Set<String>,
        fields: List<Map.Entry<String, RelDataType>>,
        includeSystemVars: Boolean
    ): Boolean {
        val scope: SelectScope = getWhereScope(select) as SelectScope
        if (expandStar(
                selectItems, aliases, fields, includeSystemVars, scope,
                selectItem
            )
        ) {
            return true
        }

        // Expand the select item: fully-qualify columns, and convert
        // parentheses-free functions such as LOCALTIME into explicit function
        // calls.
        var expanded: SqlNode = expandSelectExpr(selectItem, scope, select)
        val alias = deriveAliasNonNull(
            selectItem,
            aliases.size()
        )

        // If expansion has altered the natural alias, supply an explicit 'AS'.
        val selectScope: SqlValidatorScope = getSelectScope(select)
        if (expanded !== selectItem) {
            val newAlias = deriveAliasNonNull(
                expanded,
                aliases.size()
            )
            if (!Objects.equals(newAlias, alias)) {
                expanded = SqlStdOperatorTable.AS.createCall(
                    selectItem.getParserPosition(),
                    expanded,
                    SqlIdentifier(alias, SqlParserPos.ZERO)
                )
                deriveTypeImpl(selectScope, expanded)
            }
        }
        selectItems.add(expanded)
        aliases.add(alias)
        if (expanded != null) {
            inferUnknownTypes(targetType, scope, expanded)
        }
        val type: RelDataType? = deriveType(selectScope, expanded)
        setValidatedNodeType(expanded, type)
        fields.add(Pair.of(alias, type))
        return false
    }

    /** Returns the set of field names in the join condition specified by USING
     * or implicitly by NATURAL, de-duplicated and in order.  */
    @Nullable
    fun usingNames(join: SqlJoin): List<String>? {
        when (join.getConditionType()) {
            USING -> {
                val list: ImmutableList.Builder<String> = ImmutableList.builder()
                val names: Set<String> = catalogReader.nameMatcher().createSet()
                for (name in SqlIdentifier.simpleNames(getCondition(join) as SqlNodeList?)) {
                    if (names.add(name)) {
                        list.add(name)
                    }
                }
                return list.build()
            }
            NONE -> if (join.isNatural()) {
                val t0: RelDataType = getValidatedNodeType(join.getLeft())
                val t1: RelDataType = getValidatedNodeType(join.getRight())
                return SqlValidatorUtil.deriveNaturalJoinColumnList(
                    catalogReader.nameMatcher(), t0, t1
                )
            }
            else -> {}
        }
        return null
    }

    private fun expandStar(
        selectItems: List<SqlNode>, aliases: Set<String>,
        fields: List<Map.Entry<String, RelDataType>>, includeSystemVars: Boolean,
        scope: SelectScope, node: SqlNode
    ): Boolean {
        if (node !is SqlIdentifier) {
            return false
        }
        val identifier: SqlIdentifier = node as SqlIdentifier
        if (!identifier.isStar()) {
            return false
        }
        val startPosition: SqlParserPos = identifier.getParserPosition()
        return when (identifier.names.size()) {
            1 -> {
                var hasDynamicStruct = false
                for (child in scope.children) {
                    val before: Int = fields.size()
                    if (child.namespace.getRowType().isDynamicStruct()) {
                        hasDynamicStruct = true
                        // don't expand star if the underneath table is dynamic.
                        // Treat this star as a special field in validation/conversion and
                        // wait until execution time to expand this star.
                        val exp: SqlNode = SqlIdentifier(
                            ImmutableList.of(
                                child.name,
                                DynamicRecordType.DYNAMIC_STAR_PREFIX
                            ),
                            startPosition
                        )
                        addToSelectList(
                            selectItems,
                            aliases,
                            fields,
                            exp,
                            scope,
                            includeSystemVars
                        )
                    } else {
                        val from: SqlNode = SqlNonNullableAccessors.getNode(child)
                        val fromNs: SqlValidatorNamespace = getNamespaceOrThrow(from, scope)
                        val rowType: RelDataType = fromNs.getRowType()
                        for (field in rowType.getFieldList()) {
                            val columnName: String = field.getName()

                            // TODO: do real implicit collation here
                            val exp = SqlIdentifier(
                                ImmutableList.of(child.name, columnName),
                                startPosition
                            )
                            // Don't add expanded rolled up columns
                            if (!isRolledUpColumn(exp, scope)) {
                                addOrExpandField(
                                    selectItems,
                                    aliases,
                                    fields,
                                    includeSystemVars,
                                    scope,
                                    exp,
                                    field
                                )
                            }
                        }
                    }
                    if (child.nullable) {
                        var i = before
                        while (i < fields.size()) {
                            val entry: Map.Entry<String, RelDataType> = fields[i]
                            val type: RelDataType = entry.getValue()
                            if (!type.isNullable()) {
                                fields.set(
                                    i,
                                    Pair.of(
                                        entry.getKey(),
                                        typeFactory.createTypeWithNullability(type, true)
                                    )
                                )
                            }
                            i++
                        }
                    }
                }
                // If NATURAL JOIN or USING is present, move key fields to the front of
                // the list, per standard SQL. Disabled if there are dynamic fields.
                if (!hasDynamicStruct || Bug.CALCITE_2400_FIXED) {
                    val from: SqlNode = requireNonNull(
                        scope.getNode().getFrom()
                    ) { "getFrom for " + scope.getNode() }
                    Permute(from, 0).permute(selectItems, fields)
                }
                true
            }
            else -> {
                val prefixId: SqlIdentifier = identifier.skipLast(1)
                val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
                val nameMatcher: SqlNameMatcher = scope.validator.catalogReader.nameMatcher()
                scope.resolve(prefixId.names, nameMatcher, true, resolved)
                if (resolved.count() === 0) {
                    // e.g. "select s.t.* from e"
                    // or "select r.* from e"
                    throw newValidationError(
                        prefixId,
                        RESOURCE.unknownIdentifier(prefixId.toString())
                    )
                }
                val rowType: RelDataType = resolved.only().rowType()
                if (rowType.isDynamicStruct()) {
                    // don't expand star if the underneath table is dynamic.
                    addToSelectList(
                        selectItems,
                        aliases,
                        fields,
                        prefixId.plus(DynamicRecordType.DYNAMIC_STAR_PREFIX, startPosition),
                        scope,
                        includeSystemVars
                    )
                } else if (rowType.isStruct()) {
                    for (field in rowType.getFieldList()) {
                        val columnName: String = field.getName()

                        // TODO: do real implicit collation here
                        addOrExpandField(
                            selectItems,
                            aliases,
                            fields,
                            includeSystemVars,
                            scope,
                            prefixId.plus(columnName, startPosition),
                            field
                        )
                    }
                } else {
                    throw newValidationError(prefixId, RESOURCE.starRequiresRecordType())
                }
                true
            }
        }
    }

    private fun maybeCast(
        node: SqlNode, currentType: RelDataType,
        desiredType: RelDataType
    ): SqlNode {
        return if (SqlTypeUtil.equalSansNullability(
                typeFactory,
                currentType,
                desiredType
            )
        ) node else SqlStdOperatorTable.CAST.createCall(
            SqlParserPos.ZERO,
            node, SqlTypeUtil.convertTypeToSpec(desiredType)
        )
    }

    private fun addOrExpandField(
        selectItems: List<SqlNode>, aliases: Set<String>,
        fields: List<Map.Entry<String, RelDataType>>, includeSystemVars: Boolean,
        scope: SelectScope, id: SqlIdentifier, field: RelDataTypeField
    ): Boolean {
        when (field.getType().getStructKind()) {
            PEEK_FIELDS, PEEK_FIELDS_DEFAULT -> {
                val starExp: SqlNode = id.plusStar()
                expandStar(
                    selectItems,
                    aliases,
                    fields,
                    includeSystemVars,
                    scope,
                    starExp
                )
                return true
            }
            else -> addToSelectList(
                selectItems,
                aliases,
                fields,
                id,
                scope,
                includeSystemVars
            )
        }
        return false
    }

    @Override
    override fun validate(topNode: SqlNode): SqlNode? {
        var scope: SqlValidatorScope = EmptyScope(this)
        scope = CatalogScope(scope, ImmutableList.of("CATALOG"))
        val topNode2: SqlNode? = validateScopedExpression(topNode, scope)
        val type: RelDataType = getValidatedNodeType(topNode2)
        Util.discard(type)
        return topNode2
    }

    @Override
    override fun lookupHints(topNode: SqlNode?, pos: SqlParserPos?): List<SqlMoniker> {
        val scope: SqlValidatorScope = EmptyScope(this)
        val outermostNode: SqlNode? = performUnconditionalRewrites(topNode, false)
        cursorSet.add(outermostNode)
        if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
            registerQuery(
                scope,
                null,
                outermostNode,
                outermostNode,
                null,
                false
            )
        }
        val ns: SqlValidatorNamespace =
            getNamespace(outermostNode) ?: throw AssertionError("Not a query: $outermostNode")
        val hintList: Collection<SqlMoniker> = Sets.newTreeSet(SqlMoniker.COMPARATOR)
        lookupSelectHints(ns, pos, hintList)
        return ImmutableList.copyOf(hintList)
    }

    @Override
    @Nullable
    override fun lookupQualifiedName(topNode: SqlNode?, pos: SqlParserPos): SqlMoniker? {
        val posString: String = pos.toString()
        val info = idPositions[posString]
        return if (info != null) {
            val qualified: SqlQualified = info.scope!!.fullyQualify(info.id)
            SqlIdentifierMoniker(qualified.identifier)
        } else {
            null
        }
    }

    /**
     * Looks up completion hints for a syntactically correct select SQL that has
     * been parsed into an expression tree.
     *
     * @param select   the Select node of the parsed expression tree
     * @param pos      indicates the position in the sql statement we want to get
     * completion hints for
     * @param hintList list of [SqlMoniker] (sql identifiers) that can
     * fill in at the indicated position
     */
    fun lookupSelectHints(
        select: SqlSelect,
        pos: SqlParserPos,
        hintList: Collection<SqlMoniker>
    ) {
        val info = idPositions[pos.toString()]
        if (info == null || info.scope == null) {
            val fromNode: SqlNode = select.getFrom()
            val fromScope: SqlValidatorScope = getFromScope(select)
            lookupFromHints(fromNode, fromScope, pos, hintList)
        } else {
            lookupNameCompletionHints(
                info.scope, info.id.names,
                info.id.getParserPosition(), hintList
            )
        }
    }

    private fun lookupSelectHints(
        ns: SqlValidatorNamespace,
        pos: SqlParserPos,
        hintList: Collection<SqlMoniker>
    ) {
        val node: SqlNode = ns.getNode()
        if (node is SqlSelect) {
            lookupSelectHints(node as SqlSelect, pos, hintList)
        }
    }

    private fun lookupFromHints(
        @Nullable node: SqlNode?,
        @Nullable scope: SqlValidatorScope,
        pos: SqlParserPos,
        hintList: Collection<SqlMoniker>
    ) {
        if (node == null) {
            // This can happen in cases like "select * _suggest_", so from clause is absent
            return
        }
        val ns: SqlValidatorNamespace = getNamespaceOrThrow(node)
        if (ns.isWrapperFor(IdentifierNamespace::class.java)) {
            val idNs: IdentifierNamespace = ns.unwrap(IdentifierNamespace::class.java)
            val id: SqlIdentifier = idNs.getId()
            for (i in 0 until id.names.size()) {
                if (pos.toString().equals(
                        id.getComponent(i).getParserPosition().toString()
                    )
                ) {
                    val objNames: List<SqlMoniker> = ArrayList()
                    SqlValidatorUtil.getSchemaObjectMonikers(
                        getCatalogReader(),
                        id.names.subList(0, i + 1),
                        objNames
                    )
                    for (objName in objNames) {
                        if (objName.getType() !== SqlMonikerType.FUNCTION) {
                            hintList.add(objName)
                        }
                    }
                    return
                }
            }
        }
        when (node.getKind()) {
            JOIN -> lookupJoinHints(node as SqlJoin, scope, pos, hintList)
            else -> lookupSelectHints(ns, pos, hintList)
        }
    }

    private fun lookupJoinHints(
        join: SqlJoin,
        @Nullable scope: SqlValidatorScope,
        pos: SqlParserPos,
        hintList: Collection<SqlMoniker>
    ) {
        val left: SqlNode = join.getLeft()
        val right: SqlNode = join.getRight()
        val condition: SqlNode = join.getCondition()
        lookupFromHints(left, scope, pos, hintList)
        if (hintList.size() > 0) {
            return
        }
        lookupFromHints(right, scope, pos, hintList)
        if (hintList.size() > 0) {
            return
        }
        val conditionType: JoinConditionType = join.getConditionType()
        when (conditionType) {
            ON -> {
                requireNonNull(condition) { "join.getCondition() for $join" }
                    .findValidOptions(
                        this,
                        getScopeOrThrow(join),
                        pos, hintList
                    )
                return
            }
            else -> {}
        }
    }

    /**
     * Populates a list of all the valid alternatives for an identifier.
     *
     * @param scope    Validation scope
     * @param names    Components of the identifier
     * @param pos      position
     * @param hintList a list of valid options
     */
    fun lookupNameCompletionHints(
        scope: SqlValidatorScope?,
        names: List<String>,
        pos: SqlParserPos,
        hintList: Collection<SqlMoniker>
    ) {
        // Remove the last part of name - it is a dummy
        val subNames: List<String> = Util.skipLast(names)
        if (subNames.size() > 0) {
            // If there's a prefix, resolve it to a namespace.
            var ns: SqlValidatorNamespace? = null
            for (name in subNames) {
                if (ns == null) {
                    val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
                    val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
                    scope!!.resolve(ImmutableList.of(name), nameMatcher, false, resolved)
                    if (resolved.count() === 1) {
                        ns = resolved.only().namespace
                    }
                } else {
                    ns = ns.lookupChild(name)
                }
                if (ns == null) {
                    break
                }
            }
            if (ns != null) {
                val rowType: RelDataType = ns.getRowType()
                if (rowType.isStruct()) {
                    for (field in rowType.getFieldList()) {
                        hintList.add(
                            SqlMonikerImpl(
                                field.getName(),
                                SqlMonikerType.COLUMN
                            )
                        )
                    }
                }
            }

            // builtin function names are valid completion hints when the
            // identifier has only 1 name part
            findAllValidFunctionNames(names, this, hintList, pos)
        } else {
            // No prefix; use the children of the current scope (that is,
            // the aliases in the FROM clause)
            scope!!.findAliases(hintList)

            // If there's only one alias, add all child columns
            val selectScope: SelectScope = SqlValidatorUtil.getEnclosingSelectScope(scope)
            if (selectScope != null
                && selectScope.getChildren().size() === 1
            ) {
                val rowType: RelDataType = selectScope.getChildren().get(0).getRowType()
                for (field in rowType.getFieldList()) {
                    hintList.add(
                        SqlMonikerImpl(
                            field.getName(),
                            SqlMonikerType.COLUMN
                        )
                    )
                }
            }
        }
        findAllValidUdfNames(names, this, hintList)
    }

    @Override
    override fun validateParameterizedExpression(
        topNode: SqlNode,
        nameToTypeMap: Map<String?, RelDataType?>?
    ): SqlNode? {
        val scope: SqlValidatorScope = ParameterScope(this, nameToTypeMap)
        return validateScopedExpression(topNode, scope)
    }

    private fun validateScopedExpression(
        topNode: SqlNode,
        scope: SqlValidatorScope
    ): SqlNode? {
        val outermostNode: SqlNode? = performUnconditionalRewrites(topNode, false)
        cursorSet.add(outermostNode)
        top = outermostNode
        TRACER.trace("After unconditional rewrite: {}", outermostNode)
        if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
            registerQuery(scope, null, outermostNode, outermostNode, null, false)
        }
        outermostNode.validate(this, scope)
        if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
            // force type derivation so that we can provide it to the
            // caller later without needing the scope
            deriveType(scope, outermostNode)
        }
        TRACER.trace("After validation: {}", outermostNode)
        return outermostNode
    }

    @Override
    fun validateQuery(
        node: SqlNode, @Nullable scope: SqlValidatorScope,
        targetRowType: RelDataType?
    ) {
        val ns: SqlValidatorNamespace = getNamespaceOrThrow(node, scope)
        if (node.getKind() === SqlKind.TABLESAMPLE) {
            val operands: List<SqlNode> = (node as SqlCall).getOperandList()
            val sampleSpec: SqlSampleSpec = SqlLiteral.sampleValue(operands[1])
            if (sampleSpec is SqlSampleSpec.SqlTableSampleSpec) {
                validateFeature(RESOURCE.sQLFeature_T613(), node.getParserPosition())
            } else if (sampleSpec is SqlSampleSpec.SqlSubstitutionSampleSpec) {
                validateFeature(
                    RESOURCE.sQLFeatureExt_T613_Substitution(),
                    node.getParserPosition()
                )
            }
        }
        validateNamespace(ns, targetRowType)
        when (node.getKind()) {
            EXTEND ->       // Until we have a dedicated namespace for EXTEND
                deriveType(requireNonNull(scope, "scope"), node)
            else -> {}
        }
        if (node === top) {
            validateModality(node)
        }
        validateAccess(
            node,
            ns.getTable(),
            SqlAccessEnum.SELECT
        )
        validateSnapshot(node, scope, ns)
    }

    /**
     * Validates a namespace.
     *
     * @param namespace Namespace
     * @param targetRowType Desired row type, must not be null, may be the data
     * type 'unknown'.
     */
    protected fun validateNamespace(
        namespace: SqlValidatorNamespace,
        targetRowType: RelDataType?
    ) {
        namespace.validate(targetRowType)
        val node: SqlNode = namespace.getNode()
        if (node != null) {
            setValidatedNodeType(node, namespace.getType())
        }
    }

    @get:VisibleForTesting
    val emptyScope: org.apache.calcite.sql.validate.SqlValidatorScope
        get() = EmptyScope(this)

    private fun getScope(select: SqlSelect, clause: Clause): SqlValidatorScope {
        return requireNonNull(
            clauseScopes[IdPair.of(select, clause)]
        ) { "no $clause scope for $select" }
    }

    fun getCursorScope(select: SqlSelect): SqlValidatorScope {
        return getScope(select, Clause.CURSOR)
    }

    @Override
    override fun getWhereScope(select: SqlSelect): SqlValidatorScope {
        return getScope(select, Clause.WHERE)
    }

    @Override
    override fun getSelectScope(select: SqlSelect): SqlValidatorScope {
        return getScope(select, Clause.SELECT)
    }

    @Override
    @Nullable
    override fun getRawSelectScope(select: SqlSelect?): SelectScope? {
        var scope: SqlValidatorScope? = clauseScopes[IdPair.of(select, Clause.SELECT)]
        if (scope is AggregatingSelectScope) {
            scope = (scope as AggregatingSelectScope?).getParent()
        }
        return scope as SelectScope?
    }

    private fun getRawSelectScopeNonNull(select: SqlSelect): SelectScope {
        return requireNonNull(
            getRawSelectScope(select)
        ) { "getRawSelectScope for $select" }
    }

    @Override
    override fun getHavingScope(select: SqlSelect): SqlValidatorScope {
        // Yes, it's the same as getSelectScope
        return getScope(select, Clause.SELECT)
    }

    @Override
    override fun getGroupScope(select: SqlSelect): SqlValidatorScope {
        // Yes, it's the same as getWhereScope
        return getScope(select, Clause.WHERE)
    }

    @Override
    @Nullable
    override fun getFromScope(select: SqlSelect?): SqlValidatorScope {
        return scopes.get(select)
    }

    @Override
    override fun getOrderScope(select: SqlSelect): SqlValidatorScope {
        return getScope(select, Clause.ORDER)
    }

    @Override
    override fun getMatchRecognizeScope(node: SqlMatchRecognize): SqlValidatorScope {
        return getScopeOrThrow(node)
    }

    @Override
    @Nullable
    override fun getJoinScope(node: SqlNode?): SqlValidatorScope {
        return scopes.get(stripAs(node))
    }

    @Override
    override fun getOverScope(node: SqlNode): SqlValidatorScope {
        return getScopeOrThrow(node)
    }

    private fun getScopeOrThrow(node: SqlNode): SqlValidatorScope {
        return requireNonNull(scopes.get(node)) { "scope for $node" }
    }

    @Nullable
    private fun getNamespace(
        node: SqlNode,
        @Nullable scope: SqlValidatorScope
    ): SqlValidatorNamespace {
        if (node is SqlIdentifier && scope is DelegatingScope) {
            val id: SqlIdentifier = node as SqlIdentifier
            val idScope: DelegatingScope = (scope as DelegatingScope).getParent() as DelegatingScope
            return getNamespace(id, idScope)
        } else if (node is SqlCall) {
            // Handle extended identifiers.
            val call: SqlCall = node as SqlCall
            when (call.getOperator().getKind()) {
                TABLE_REF -> return getNamespace(call.operand(0), scope)
                EXTEND -> {
                    val operand0: SqlNode = call.getOperandList().get(0)
                    val identifier: SqlIdentifier =
                        if (operand0.getKind() === SqlKind.TABLE_REF) (operand0 as SqlCall).operand(0) else operand0 as SqlIdentifier
                    val idScope: DelegatingScope = scope as DelegatingScope
                    return getNamespace(identifier, idScope)
                }
                AS -> {
                    val nested: SqlNode = call.getOperandList().get(0)
                    when (nested.getKind()) {
                        TABLE_REF, EXTEND -> return getNamespace(nested, scope)
                        else -> {}
                    }
                }
                else -> {}
            }
        }
        return getNamespace(node)
    }

    @Nullable
    private fun getNamespace(
        id: SqlIdentifier,
        @Nullable scope: DelegatingScope
    ): SqlValidatorNamespace {
        if (id.isSimple()) {
            val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
            val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
            requireNonNull(scope) { "scope needed to lookup $id" }
                .resolve(id.names, nameMatcher, false, resolved)
            if (resolved.count() === 1) {
                return resolved.only().namespace
            }
        }
        return getNamespace(id)
    }

    @Override
    @Nullable
    override fun getNamespace(node: SqlNode?): SqlValidatorNamespace {
        return when (node.getKind()) {
            AS -> {

                // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
                val ns: SqlValidatorNamespace = namespaces.get(node)
                if (ns != null) {
                    ns
                } else getNamespace((node as SqlCall?).operand(0))
            }
            TABLE_REF, SNAPSHOT, OVER, COLLECTION_TABLE, ORDER_BY, TABLESAMPLE -> getNamespace(
                (node as SqlCall?).operand(
                    0
                )
            )
            else -> namespaces.get(node)
        }
    }

    /**
     * Namespace for the given node.
     * @param node node to compute the namespace for
     * @return namespace for the given node, never null
     * @see .getNamespace
     */
    @API(since = "1.27", status = API.Status.INTERNAL)
    fun getNamespaceOrThrow(node: SqlNode): SqlValidatorNamespace {
        return requireNonNull(
            getNamespace(node)
        ) { "namespace for $node" }
    }

    /**
     * Namespace for the given node.
     * @param node node to compute the namespace for
     * @param scope namespace scope
     * @return namespace for the given node, never null
     * @see .getNamespace
     */
    @API(since = "1.27", status = API.Status.INTERNAL)
    fun getNamespaceOrThrow(
        node: SqlNode,
        @Nullable scope: SqlValidatorScope
    ): SqlValidatorNamespace {
        return requireNonNull(
            getNamespace(node, scope)
        ) { "namespace for $node, scope $scope" }
    }

    /**
     * Namespace for the given node.
     * @param id identifier to resolve
     * @param scope namespace scope
     * @return namespace for the given node, never null
     * @see .getNamespace
     */
    @API(since = "1.26", status = API.Status.INTERNAL)
    fun getNamespaceOrThrow(
        id: SqlIdentifier,
        @Nullable scope: DelegatingScope
    ): SqlValidatorNamespace {
        return requireNonNull(
            getNamespace(id, scope)
        ) { "namespace for $id, scope $scope" }
    }

    private fun handleOffsetFetch(@Nullable offset: SqlNode, @Nullable fetch: SqlNode) {
        if (offset is SqlDynamicParam) {
            setValidatedNodeType(
                offset,
                typeFactory.createSqlType(SqlTypeName.INTEGER)
            )
        }
        if (fetch is SqlDynamicParam) {
            setValidatedNodeType(
                fetch,
                typeFactory.createSqlType(SqlTypeName.INTEGER)
            )
        }
    }

    /**
     * Performs expression rewrites which are always used unconditionally. These
     * rewrites massage the expression tree into a standard form so that the
     * rest of the validation logic can be simpler.
     *
     *
     * Returns null if and only if the original expression is null.
     *
     * @param node      expression to be rewritten
     * @param underFrom whether node appears directly under a FROM clause
     * @return rewritten expression, or null if the original expression is null
     */
    @PolyNull
    protected fun performUnconditionalRewrites(
        @PolyNull node: SqlNode?,
        underFrom: Boolean
    ): SqlNode? {
        var node: SqlNode = node ?: return null

        // first transform operands and invoke generic call rewrite
        if (node is SqlCall) {
            if (node is SqlMerge) {
                validatingSqlMerge = true
            }
            val call: SqlCall = node as SqlCall
            val kind: SqlKind = call.getKind()
            val operands: List<SqlNode> = call.getOperandList()
            for (i in 0 until operands.size()) {
                val operand: SqlNode = operands[i]
                var childUnderFrom: Boolean
                childUnderFrom = if (kind === SqlKind.SELECT) {
                    i == SqlSelect.FROM_OPERAND
                } else if (kind === SqlKind.AS && i == 0) {
                    // for an aliased expression, it is under FROM if
                    // the AS expression is under FROM
                    underFrom
                } else {
                    false
                }
                val newOperand: SqlNode? = performUnconditionalRewrites(operand, childUnderFrom)
                if (newOperand != null && newOperand !== operand) {
                    call.setOperand(i, newOperand)
                }
            }
            if (call.getOperator() is SqlUnresolvedFunction) {
                assert(call is SqlBasicCall)
                val function: SqlUnresolvedFunction = call.getOperator() as SqlUnresolvedFunction
                // This function hasn't been resolved yet.  Perform
                // a half-hearted resolution now in case it's a
                // builtin function requiring special casing.  If it's
                // not, we'll handle it later during overload resolution.
                val overloads: List<SqlOperator> = ArrayList()
                opTab.lookupOperatorOverloads(
                    function.getNameAsId(),
                    function.getFunctionType(), SqlSyntax.FUNCTION, overloads,
                    catalogReader.nameMatcher()
                )
                if (overloads.size() === 1) {
                    (call as SqlBasicCall).setOperator(overloads[0])
                }
            }
            if (config.callRewrite()) {
                node = call.getOperator().rewriteCall(this, call)
            }
        } else if (node is SqlNodeList) {
            val list: SqlNodeList = node as SqlNodeList
            for (i in 0 until list.size()) {
                val operand: SqlNode = list.get(i)
                val newOperand: SqlNode? = performUnconditionalRewrites(
                    operand,
                    false
                )
                if (newOperand != null) {
                    list.set(i, newOperand)
                }
            }
        }

        // now transform node itself
        val kind: SqlKind = node.getKind()
        when (kind) {
            VALUES ->       // Do not rewrite VALUES clauses.
                // At some point we used to rewrite VALUES(...) clauses
                // to (SELECT * FROM VALUES(...)) but this was problematic
                // in various cases such as FROM (VALUES(...)) [ AS alias ]
                // where the rewrite was invoked over and over making the
                // expression grow indefinitely.
                return node
            ORDER_BY -> {
                val orderBy: SqlOrderBy = node as SqlOrderBy
                handleOffsetFetch(orderBy.offset, orderBy.fetch)
                if (orderBy.query is SqlSelect) {
                    val select: SqlSelect = orderBy.query as SqlSelect

                    // Don't clobber existing ORDER BY.  It may be needed for
                    // an order-sensitive function like RANK.
                    if (select.getOrderList() == null) {
                        // push ORDER BY into existing select
                        select.setOrderBy(orderBy.orderList)
                        select.setOffset(orderBy.offset)
                        select.setFetch(orderBy.fetch)
                        return select
                    }
                }
                if (orderBy.query is SqlWith
                    && (orderBy.query as SqlWith).body is SqlSelect
                ) {
                    val with: SqlWith = orderBy.query as SqlWith
                    val select: SqlSelect = with.body as SqlSelect

                    // Don't clobber existing ORDER BY.  It may be needed for
                    // an order-sensitive function like RANK.
                    if (select.getOrderList() == null) {
                        // push ORDER BY into existing select
                        select.setOrderBy(orderBy.orderList)
                        select.setOffset(orderBy.offset)
                        select.setFetch(orderBy.fetch)
                        return with
                    }
                }
                val selectList = SqlNodeList(SqlParserPos.ZERO)
                selectList.add(SqlIdentifier.star(SqlParserPos.ZERO))
                val orderList: SqlNodeList
                val innerSelect: SqlSelect? = getInnerSelect(node)
                if (innerSelect != null && isAggregate(innerSelect)) {
                    orderList = SqlNode.clone(orderBy.orderList)
                    // We assume that ORDER BY item does not have ASC etc.
                    // We assume that ORDER BY item is present in SELECT list.
                    var i = 0
                    while (i < orderList.size()) {
                        val sqlNode: SqlNode = orderList.get(i)
                        val selectList2: SqlNodeList = SqlNonNullableAccessors.getSelectList(innerSelect)
                        for (sel in Ord.zip(selectList2)) {
                            if (stripAs(sel.e).equalsDeep(sqlNode, Litmus.IGNORE)) {
                                orderList.set(
                                    i,
                                    SqlLiteral.createExactNumeric(
                                        Integer.toString(sel.i + 1),
                                        SqlParserPos.ZERO
                                    )
                                )
                            }
                        }
                        i++
                    }
                } else {
                    orderList = orderBy.orderList
                }
                return SqlSelect(
                    SqlParserPos.ZERO, null, selectList, orderBy.query,
                    null, null, null, null, orderList, orderBy.offset,
                    orderBy.fetch, null
                )
            }
            EXPLICIT_TABLE -> {

                // (TABLE t) is equivalent to (SELECT * FROM t)
                val call: SqlCall = node as SqlCall
                val selectList = SqlNodeList(SqlParserPos.ZERO)
                selectList.add(SqlIdentifier.star(SqlParserPos.ZERO))
                return SqlSelect(
                    SqlParserPos.ZERO, null, selectList, call.operand(0),
                    null, null, null, null, null, null, null, null
                )
            }
            DELETE -> {
                val call: SqlDelete = node as SqlDelete
                val select: SqlSelect = createSourceSelectForDelete(call)
                call.setSourceSelect(select)
            }
            UPDATE -> {
                val call: SqlUpdate = node as SqlUpdate
                val select: SqlSelect = createSourceSelectForUpdate(call)
                call.setSourceSelect(select)

                // See if we're supposed to rewrite UPDATE to MERGE
                // (unless this is the UPDATE clause of a MERGE,
                // in which case leave it alone).
                if (!validatingSqlMerge) {
                    val selfJoinSrcExpr: SqlNode? = getSelfJoinExprForUpdate(
                        call.getTargetTable(),
                        UPDATE_SRC_ALIAS
                    )
                    if (selfJoinSrcExpr != null) {
                        node = rewriteUpdateToMerge(call, selfJoinSrcExpr)
                    }
                }
            }
            MERGE -> {
                val call: SqlMerge = node as SqlMerge
                rewriteMerge(call)
            }
            else -> {}
        }
        return node
    }

    private fun rewriteUpdateToMerge(
        updateCall: SqlUpdate,
        selfJoinSrcExpr: SqlNode
    ): SqlNode {
        // Make sure target has an alias.
        var updateAlias: SqlIdentifier? = updateCall.getAlias()
        if (updateAlias == null) {
            updateAlias = SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO)
            updateCall.setAlias(updateAlias)
        }
        val selfJoinTgtExpr: SqlNode? = getSelfJoinExprForUpdate(
            updateCall.getTargetTable(),
            updateAlias.getSimple()
        )
        assert(selfJoinTgtExpr != null)

        // Create join condition between source and target exprs,
        // creating a conjunction with the user-level WHERE
        // clause if one was supplied
        var condition: SqlNode = updateCall.getCondition()
        val selfJoinCond: SqlNode = SqlStdOperatorTable.EQUALS.createCall(
            SqlParserPos.ZERO,
            selfJoinSrcExpr,
            selfJoinTgtExpr
        )
        condition = if (condition == null) {
            selfJoinCond
        } else {
            SqlStdOperatorTable.AND.createCall(
                SqlParserPos.ZERO,
                selfJoinCond,
                condition
            )
        }
        val target: SqlNode = updateCall.getTargetTable().clone(SqlParserPos.ZERO)

        // For the source, we need to anonymize the fields, so
        // that for a statement like UPDATE T SET I = I + 1,
        // there's no ambiguity for the "I" in "I + 1";
        // this is OK because the source and target have
        // identical values due to the self-join.
        // Note that we anonymize the source rather than the
        // target because downstream, the optimizer rules
        // don't want to see any projection on top of the target.
        val ns = IdentifierNamespace(
            this, target, null,
            castNonNull(null)
        )
        val rowType: RelDataType = ns.getRowType()
        var source: SqlNode = updateCall.getTargetTable().clone(SqlParserPos.ZERO)
        val selectList = SqlNodeList(SqlParserPos.ZERO)
        var i = 1
        for (field in rowType.getFieldList()) {
            val col = SqlIdentifier(
                field.getName(),
                SqlParserPos.ZERO
            )
            selectList.add(
                SqlValidatorUtil.addAlias(col, UPDATE_ANON_PREFIX + i)
            )
            ++i
        }
        source = SqlSelect(
            SqlParserPos.ZERO, null, selectList, source, null, null,
            null, null, null, null, null, null
        )
        source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS)
        val mergeCall = SqlMerge(
            updateCall.getParserPosition(), target, condition, source,
            updateCall, null, null, updateCall.getAlias()
        )
        rewriteMerge(mergeCall)
        return mergeCall
    }

    /**
     * Allows a subclass to provide information about how to convert an UPDATE
     * into a MERGE via self-join. If this method returns null, then no such
     * conversion takes place. Otherwise, this method should return a suitable
     * unique identifier expression for the given table.
     *
     * @param table identifier for table being updated
     * @param alias alias to use for qualifying columns in expression, or null
     * for unqualified references; if this is equal to
     * {@value #UPDATE_SRC_ALIAS}, then column references have been
     * anonymized to "SYS$ANONx", where x is the 1-based column
     * number.
     * @return expression for unique identifier, or null to prevent conversion
     */
    @Nullable
    protected fun getSelfJoinExprForUpdate(
        table: SqlNode?,
        alias: String?
    ): SqlNode? {
        return null
    }

    /**
     * Creates the SELECT statement that putatively feeds rows into an UPDATE
     * statement to be updated.
     *
     * @param call Call to the UPDATE operator
     * @return select statement
     */
    protected fun createSourceSelectForUpdate(call: SqlUpdate?): SqlSelect {
        val selectList = SqlNodeList(SqlParserPos.ZERO)
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO))
        var ordinal = 0
        for (exp in call.getSourceExpressionList()) {
            // Force unique aliases to avoid a duplicate for Y with
            // SET X=Y
            val alias: String = SqlUtil.deriveAliasFromOrdinal(ordinal)
            selectList.add(SqlValidatorUtil.addAlias(exp, alias))
            ++ordinal
        }
        var sourceTable: SqlNode = call.getTargetTable()
        val alias: SqlIdentifier = call.getAlias()
        if (alias != null) {
            sourceTable = SqlValidatorUtil.addAlias(
                sourceTable,
                alias.getSimple()
            )
        }
        return SqlSelect(
            SqlParserPos.ZERO, null, selectList, sourceTable,
            call.getCondition(), null, null, null, null, null, null, null
        )
    }

    /**
     * Creates the SELECT statement that putatively feeds rows into a DELETE
     * statement to be deleted.
     *
     * @param call Call to the DELETE operator
     * @return select statement
     */
    protected fun createSourceSelectForDelete(call: SqlDelete?): SqlSelect {
        val selectList = SqlNodeList(SqlParserPos.ZERO)
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO))
        var sourceTable: SqlNode = call.getTargetTable()
        val alias: SqlIdentifier = call.getAlias()
        if (alias != null) {
            sourceTable = SqlValidatorUtil.addAlias(
                sourceTable,
                alias.getSimple()
            )
        }
        return SqlSelect(
            SqlParserPos.ZERO, null, selectList, sourceTable,
            call.getCondition(), null, null, null, null, null, null, null
        )
    }

    /**
     * Returns null if there is no common type. E.g. if the rows have a
     * different number of columns.
     */
    @Nullable
    fun getTableConstructorRowType(
        values: SqlCall,
        scope: SqlValidatorScope
    ): RelDataType {
        val rows: List<SqlNode> = values.getOperandList()
        assert(rows.size() >= 1)
        val rowTypes: List<RelDataType> = ArrayList()
        for (row in rows) {
            assert(row.getKind() === SqlKind.ROW)
            val rowConstructor: SqlCall = row as SqlCall

            // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
            // rows, need to infer aliases from there.
            val aliasList: List<String> = ArrayList()
            val typeList: List<RelDataType> = ArrayList()
            for (column in Ord.zip(rowConstructor.getOperandList())) {
                val alias = deriveAliasNonNull(column.e, column.i)
                aliasList.add(alias)
                val type: RelDataType? = deriveType(scope, column.e)
                typeList.add(type)
            }
            rowTypes.add(typeFactory.createStructType(typeList, aliasList))
        }
        return if (rows.size() === 1) {
            // TODO jvs 10-Oct-2005:  get rid of this workaround once
            // leastRestrictive can handle all cases
            rowTypes[0]
        } else typeFactory.leastRestrictive(rowTypes)
    }

    @Override
    override fun getValidatedNodeType(node: SqlNode?): RelDataType {
        val type: RelDataType? = getValidatedNodeTypeIfKnown(node)
        return if (type == null) {
            if (node.getKind() === SqlKind.IDENTIFIER) {
                throw newValidationError(node, RESOURCE.unknownIdentifier(node.toString()))
            }
            throw Util.needToImplement(node)
        } else {
            type
        }
    }

    @Override
    @Nullable
    override fun getValidatedNodeTypeIfKnown(node: SqlNode?): RelDataType? {
        val type: RelDataType = nodeToTypeMap.get(node)
        if (type != null) {
            return type
        }
        val ns: SqlValidatorNamespace = getNamespace(node)
        if (ns != null) {
            return ns.getType()
        }
        val original: SqlNode? = originalExprs[node]
        if (original != null && original !== node) {
            return getValidatedNodeType(original)
        }
        return if (node is SqlIdentifier) {
            getCatalogReader().getNamedType(node as SqlIdentifier?)
        } else null
    }

    @Override
    @Nullable
    override fun getValidatedOperandTypes(call: SqlCall?): List<RelDataType> {
        return callToOperandTypesMap.get(call)
    }

    /**
     * Saves the type of a [SqlNode], now that it has been validated.
     *
     *
     * Unlike the base class method, this method is not deprecated.
     * It is available from within Calcite, but is not part of the public API.
     *
     * @param node A SQL parse tree node, never null
     * @param type Its type; must not be null
     */
    @Override
    override fun setValidatedNodeType(node: SqlNode?, type: RelDataType?) {
        requireNonNull(type, "type")
        requireNonNull(node, "node")
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return
        }
        nodeToTypeMap.put(node, type)
    }

    @Override
    override fun removeValidatedNodeType(node: SqlNode?) {
        nodeToTypeMap.remove(node)
    }

    @Override
    @Nullable
    override fun makeNullaryCall(id: SqlIdentifier?): SqlCall? {
        if (id.names.size() === 1 && !id.isComponentQuoted(0)) {
            val list: List<SqlOperator> = ArrayList()
            opTab.lookupOperatorOverloads(
                id, null, SqlSyntax.FUNCTION, list,
                catalogReader.nameMatcher()
            )
            for (operator in list) {
                if (operator.getSyntax() === SqlSyntax.FUNCTION_ID) {
                    // Even though this looks like an identifier, it is a
                    // actually a call to a function. Construct a fake
                    // call to this function, so we can use the regular
                    // operator validation.
                    return SqlBasicCall(
                        operator, ImmutableList.of(),
                        id.getParserPosition(), null
                    ).withExpanded(true)
                }
            }
        }
        return null
    }

    @Override
    fun deriveType(
        scope: SqlValidatorScope,
        expr: SqlNode?
    ): RelDataType? {
        requireNonNull(scope, "scope")
        requireNonNull(expr, "expr")

        // if we already know the type, no need to re-derive
        var type: RelDataType = nodeToTypeMap.get(expr)
        if (type != null) {
            return type
        }
        val ns: SqlValidatorNamespace = getNamespace(expr)
        if (ns != null) {
            return ns.getType()
        }
        type = deriveTypeImpl(scope, expr)
        Preconditions.checkArgument(
            type != null,
            "SqlValidator.deriveTypeInternal returned null"
        )
        setValidatedNodeType(expr, type)
        return type
    }

    /**
     * Derives the type of a node, never null.
     */
    fun deriveTypeImpl(
        scope: SqlValidatorScope,
        operand: SqlNode?
    ): RelDataType {
        val v: DeriveTypeVisitor = DeriveTypeVisitor(scope)
        val type: RelDataType = operand.accept(v)
        return requireNonNull(scope.nullifyType(operand, type))
    }

    @Override
    override fun deriveConstructorType(
        scope: SqlValidatorScope?,
        call: SqlCall,
        unresolvedConstructor: SqlFunction,
        @Nullable resolvedConstructor: SqlFunction?,
        argTypes: List<RelDataType?>?
    ): RelDataType? {
        val sqlIdentifier: SqlIdentifier = unresolvedConstructor.getSqlIdentifier()
        assert(sqlIdentifier != null)
        val type: RelDataType = catalogReader.getNamedType(sqlIdentifier)
            ?: // TODO jvs 12-Feb-2005:  proper type name formatting
            throw newValidationError(
                sqlIdentifier,
                RESOURCE.unknownDatatypeName(sqlIdentifier.toString())
            )
        if (resolvedConstructor == null) {
            if (call.operandCount() > 0) {
                // This is not a default constructor invocation, and
                // no user-defined constructor could be found
                throw handleUnresolvedFunction(
                    call, unresolvedConstructor, argTypes,
                    null
                )
            }
        } else {
            val testCall: SqlCall = resolvedConstructor.createCall(
                call.getParserPosition(),
                call.getOperandList()
            )
            val returnType: RelDataType = resolvedConstructor.validateOperands(
                this,
                scope,
                testCall
            )
            assert(type === returnType)
        }
        if (config.identifierExpansion()) {
            if (resolvedConstructor != null) {
                (call as SqlBasicCall).setOperator(resolvedConstructor)
            } else {
                // fake a fully-qualified call to the default constructor
                (call as SqlBasicCall).setOperator(
                    SqlFunction(
                        requireNonNull(type.getSqlIdentifier()) { "sqlIdentifier of $type" },
                        ReturnTypes.explicit(type),
                        null,
                        null,
                        null,
                        SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR
                    )
                )
            }
        }
        return type
    }

    @Override
    override fun handleUnresolvedFunction(
        call: SqlCall,
        unresolvedFunction: SqlOperator, argTypes: List<RelDataType?>?,
        @Nullable argNames: List<String?>?
    ): CalciteException {
        // For builtins, we can give a better error message
        val overloads: List<SqlOperator> = ArrayList()
        opTab.lookupOperatorOverloads(
            unresolvedFunction.getNameAsId(), null,
            SqlSyntax.FUNCTION, overloads, catalogReader.nameMatcher()
        )
        if (overloads.size() === 1) {
            val `fun`: SqlFunction = overloads[0] as SqlFunction
            if (`fun`.getSqlIdentifier() == null
                && `fun`.getSyntax() !== SqlSyntax.FUNCTION_ID
            ) {
                val expectedArgCount: Int = `fun`.getOperandCountRange().getMin()
                throw newValidationError(
                    call,
                    RESOURCE.invalidArgCount(
                        call.getOperator().getName(),
                        expectedArgCount
                    )
                )
            }
        }
        val signature: String
        signature = if (unresolvedFunction is SqlFunction) {
            val typeChecking: SqlOperandTypeChecker = AssignableOperandTypeChecker(argTypes, argNames)
            typeChecking.getAllowedSignatures(
                unresolvedFunction,
                unresolvedFunction.getName()
            )
        } else {
            unresolvedFunction.getName()
        }
        throw newValidationError(
            call,
            RESOURCE.validatorUnknownFunction(signature)
        )
    }

    protected fun inferUnknownTypes(
        inferredType: RelDataType?,
        scope: SqlValidatorScope,
        node: SqlNode?
    ) {
        var scope: SqlValidatorScope = scope
        requireNonNull(inferredType, "inferredType")
        requireNonNull(scope, "scope")
        requireNonNull(node, "node")
        val newScope: SqlValidatorScope = scopes.get(node)
        if (newScope != null) {
            scope = newScope
        }
        val isNullLiteral: Boolean = SqlUtil.isNullLiteral(node, false)
        if (node is SqlDynamicParam || isNullLiteral) {
            if (inferredType.equals(unknownType)) {
                if (isNullLiteral) {
                    if (config.typeCoercionEnabled()) {
                        // derive type of null literal
                        deriveType(scope, node)
                        return
                    } else {
                        throw newValidationError(node, RESOURCE.nullIllegal())
                    }
                } else {
                    throw newValidationError(node, RESOURCE.dynamicParamIllegal())
                }
            }

            // REVIEW:  should dynamic parameter types always be nullable?
            var newInferredType: RelDataType = typeFactory.createTypeWithNullability(inferredType, true)
            if (SqlTypeUtil.inCharFamily(inferredType)) {
                newInferredType = typeFactory.createTypeWithCharsetAndCollation(
                    newInferredType,
                    getCharset(inferredType),
                    getCollation(inferredType)
                )
            }
            setValidatedNodeType(node, newInferredType)
        } else if (node is SqlNodeList) {
            val nodeList: SqlNodeList? = node as SqlNodeList?
            if (inferredType.isStruct()) {
                if (inferredType.getFieldCount() !== nodeList.size()) {
                    // this can happen when we're validating an INSERT
                    // where the source and target degrees are different;
                    // bust out, and the error will be detected higher up
                    return
                }
            }
            var i = 0
            for (child in nodeList) {
                var type: RelDataType?
                if (inferredType.isStruct()) {
                    type = inferredType.getFieldList().get(i).getType()
                    ++i
                } else {
                    type = inferredType
                }
                inferUnknownTypes(type, scope, child)
            }
        } else if (node is SqlCase) {
            val caseCall: SqlCase? = node as SqlCase?
            val whenType: RelDataType = if (caseCall.getValueOperand() == null) booleanType else unknownType
            for (sqlNode in caseCall.getWhenOperands()) {
                inferUnknownTypes(whenType, scope, sqlNode)
            }
            val returnType: RelDataType? = deriveType(scope, node)
            for (sqlNode in caseCall.getThenOperands()) {
                inferUnknownTypes(returnType, scope, sqlNode)
            }
            val elseOperand: SqlNode = requireNonNull(
                caseCall.getElseOperand()
            ) { "elseOperand for $caseCall" }
            if (!SqlUtil.isNullLiteral(elseOperand, false)) {
                inferUnknownTypes(
                    returnType,
                    scope,
                    elseOperand
                )
            } else {
                setValidatedNodeType(elseOperand, returnType)
            }
        } else if (node.getKind() === SqlKind.AS) {
            // For AS operator, only infer the operand not the alias
            inferUnknownTypes(inferredType, scope, (node as SqlCall?).operand(0))
        } else if (node is SqlCall) {
            val call: SqlCall? = node as SqlCall?
            val operandTypeInference: SqlOperandTypeInference = call.getOperator().getOperandTypeInference()
            val callBinding = SqlCallBinding(this, scope, call)
            val operands: List<SqlNode> = callBinding.operands()
            val operandTypes: Array<RelDataType?> = arrayOfNulls<RelDataType>(operands.size())
            Arrays.fill(operandTypes, unknownType)
            // TODO:  eventually should assert(operandTypeInference != null)
            // instead; for now just eat it
            if (operandTypeInference != null) {
                operandTypeInference.inferOperandTypes(
                    callBinding,
                    inferredType,
                    operandTypes
                )
            }
            for (i in 0 until operands.size()) {
                val operand: SqlNode = operands[i]
                if (operand != null) {
                    inferUnknownTypes(operandTypes[i], scope, operand)
                }
            }
        }
    }

    /**
     * Adds an expression to a select list, ensuring that its alias does not
     * clash with any existing expressions on the list.
     */
    protected fun addToSelectList(
        list: List<SqlNode?>,
        aliases: Set<String?>,
        fieldList: List<Map.Entry<String?, RelDataType?>?>,
        exp: SqlNode,
        scope: SelectScope,
        includeSystemVars: Boolean
    ) {
        var exp: SqlNode = exp
        val alias: String = SqlValidatorUtil.getAlias(exp, -1)
        val uniqueAlias: String = SqlValidatorUtil.uniquify(
            alias, aliases, SqlValidatorUtil.EXPR_SUGGESTER
        )
        if (!Objects.equals(alias, uniqueAlias)) {
            exp = SqlValidatorUtil.addAlias(exp, uniqueAlias)
        }
        fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)))
        list.add(exp)
    }

    @Override
    @Nullable
    override fun deriveAlias(
        node: SqlNode?,
        ordinal: Int
    ): String? {
        return SqlValidatorUtil.getAlias(node, ordinal)
    }

    private fun deriveAliasNonNull(node: SqlNode?, ordinal: Int): String {
        return requireNonNull(
            deriveAlias(node, ordinal)
        ) { "non-null alias expected for node = $node, ordinal = $ordinal" }
    }

    protected fun shouldAllowIntermediateOrderBy(): Boolean {
        return true
    }

    private fun registerMatchRecognize(
        parentScope: SqlValidatorScope,
        usingScope: SqlValidatorScope,
        call: SqlMatchRecognize,
        enclosingNode: SqlNode,
        @Nullable alias: String?,
        forceNullable: Boolean
    ) {
        val matchRecognizeNamespace: MatchRecognizeNamespace = createMatchRecognizeNameSpace(call, enclosingNode)
        registerNamespace(usingScope, alias, matchRecognizeNamespace, forceNullable)
        val matchRecognizeScope = MatchRecognizeScope(parentScope, call)
        scopes.put(call, matchRecognizeScope)

        // parse input query
        val expr: SqlNode = call.getTableRef()
        val newExpr: SqlNode = registerFrom(
            usingScope, matchRecognizeScope, true, expr,
            expr, null, null, forceNullable, false
        )
        if (expr !== newExpr) {
            call.setOperand(0, newExpr)
        }
    }

    protected fun createMatchRecognizeNameSpace(
        call: SqlMatchRecognize?,
        enclosingNode: SqlNode?
    ): MatchRecognizeNamespace {
        return MatchRecognizeNamespace(this, call, enclosingNode)
    }

    private fun registerPivot(
        parentScope: SqlValidatorScope,
        usingScope: SqlValidatorScope,
        pivot: SqlPivot,
        enclosingNode: SqlNode,
        @Nullable alias: String?,
        forceNullable: Boolean
    ) {
        val namespace: PivotNamespace = createPivotNameSpace(pivot, enclosingNode)
        registerNamespace(usingScope, alias, namespace, forceNullable)
        val scope: SqlValidatorScope = PivotScope(parentScope, pivot)
        scopes.put(pivot, scope)

        // parse input query
        val expr: SqlNode = pivot.query
        val newExpr: SqlNode = registerFrom(
            parentScope, scope, true, expr,
            expr, null, null, forceNullable, false
        )
        if (expr !== newExpr) {
            pivot.setOperand(0, newExpr)
        }
    }

    protected fun createPivotNameSpace(
        call: SqlPivot?,
        enclosingNode: SqlNode?
    ): PivotNamespace {
        return PivotNamespace(this, call, enclosingNode)
    }

    private fun registerUnpivot(
        parentScope: SqlValidatorScope,
        usingScope: SqlValidatorScope,
        call: SqlUnpivot,
        enclosingNode: SqlNode,
        @Nullable alias: String?,
        forceNullable: Boolean
    ) {
        val namespace: UnpivotNamespace = createUnpivotNameSpace(call, enclosingNode)
        registerNamespace(usingScope, alias, namespace, forceNullable)
        val scope: SqlValidatorScope = UnpivotScope(parentScope, call)
        scopes.put(call, scope)

        // parse input query
        val expr: SqlNode = call.query
        val newExpr: SqlNode = registerFrom(
            parentScope, scope, true, expr,
            expr, null, null, forceNullable, false
        )
        if (expr !== newExpr) {
            call.setOperand(0, newExpr)
        }
    }

    protected fun createUnpivotNameSpace(
        call: SqlUnpivot?,
        enclosingNode: SqlNode?
    ): UnpivotNamespace {
        return UnpivotNamespace(this, call, enclosingNode)
    }

    /**
     * Registers a new namespace, and adds it as a child of its parent scope.
     * Derived class can override this method to tinker with namespaces as they
     * are created.
     *
     * @param usingScope    Parent scope (which will want to look for things in
     * this namespace)
     * @param alias         Alias by which parent will refer to this namespace
     * @param ns            Namespace
     * @param forceNullable Whether to force the type of namespace to be nullable
     */
    protected fun registerNamespace(
        @Nullable usingScope: SqlValidatorScope?,
        @Nullable alias: String?,
        ns: SqlValidatorNamespace,
        forceNullable: Boolean
    ) {
        namespaces.put(requireNonNull(ns.getNode()) { "ns.getNode() for $ns" }, ns)
        if (usingScope != null) {
            assert(alias != null) {
                ("Registering namespace " + ns + ", into scope " + usingScope
                        + ", so alias must not be null")
            }
            usingScope.addChild(ns, alias, forceNullable)
        }
    }

    /**
     * Registers scopes and namespaces implied a relational expression in the
     * FROM clause.
     *
     *
     * `parentScope` and `usingScope` are often the same. They
     * differ when the namespace are not visible within the parent. (Example
     * needed.)
     *
     *
     * Likewise, `enclosingNode` and `node` are often the same.
     * `enclosingNode` is the topmost node within the FROM clause, from
     * which any decorations like an alias (`AS alias`) or a table
     * sample clause are stripped away to get `node`. Both are recorded in
     * the namespace.
     *
     * @param parentScope   Parent scope which this scope turns to in order to
     * resolve objects
     * @param usingScope    Scope whose child list this scope should add itself to
     * @param register      Whether to register this scope as a child of
     * `usingScope`
     * @param node          Node which namespace is based on
     * @param enclosingNode Outermost node for namespace, including decorations
     * such as alias and sample clause
     * @param alias         Alias
     * @param extendList    Definitions of extended columns
     * @param forceNullable Whether to force the type of namespace to be
     * nullable because it is in an outer join
     * @param lateral       Whether LATERAL is specified, so that items to the
     * left of this in the JOIN tree are visible in the
     * scope
     * @return registered node, usually the same as `node`
     */
    private fun registerFrom(
        parentScope: SqlValidatorScope,
        usingScope: SqlValidatorScope,
        register: Boolean,
        node: SqlNode,
        enclosingNode: SqlNode,
        @Nullable alias: String?,
        @Nullable extendList: SqlNodeList?,
        forceNullable: Boolean,
        lateral: Boolean
    ): SqlNode {
        var parentScope: SqlValidatorScope = parentScope
        var alias = alias
        val kind: SqlKind = node.getKind()
        val expr: SqlNode
        val newExpr: SqlNode

        // Add an alias if necessary.
        var newNode: SqlNode = node
        if (alias == null) {
            when (kind) {
                IDENTIFIER, OVER -> {
                    alias = deriveAlias(node, -1)
                    if (alias == null) {
                        alias = deriveAliasNonNull(node, nextGeneratedId++)
                    }
                    if (config.identifierExpansion()) {
                        newNode = SqlValidatorUtil.addAlias(node, alias)
                    }
                }
                SELECT, UNION, INTERSECT, EXCEPT, VALUES, UNNEST, OTHER_FUNCTION, COLLECTION_TABLE, PIVOT, UNPIVOT, MATCH_RECOGNIZE -> {

                    // give this anonymous construct a name since later
                    // query processing stages rely on it
                    alias = deriveAliasNonNull(node, nextGeneratedId++)
                    if (config.identifierExpansion()) {
                        // Since we're expanding identifiers, we should make the
                        // aliases explicit too, otherwise the expanded query
                        // will not be consistent if we convert back to SQL, e.g.
                        // "select EXPR$1.EXPR$2 from values (1)".
                        newNode = SqlValidatorUtil.addAlias(node, alias)
                    }
                }
                else -> {}
            }
        }
        if (lateral) {
            var s: SqlValidatorScope = usingScope
            while (s is JoinScope) {
                s = (s as JoinScope).getUsingScope()
            }
            val node2: SqlNode = if (s != null) s.getNode() else node
            val tableScope = TableScope(parentScope, node2)
            if (usingScope is ListScope) {
                for (child in (usingScope as ListScope).children) {
                    tableScope.addChild(child.namespace, child.name, child.nullable)
                }
            }
            parentScope = tableScope
        }
        val call: SqlCall
        val operand: SqlNode
        val newOperand: SqlNode
        return when (kind) {
            AS -> {
                call = node as SqlCall
                if (alias == null) {
                    alias = String.valueOf(call.operand(1))
                }
                expr = call.operand(0)
                val needAlias =
                    call.operandCount() > 2 || expr.getKind() === SqlKind.VALUES || (expr.getKind() === SqlKind.UNNEST
                            && ((expr as SqlCall).operand(0).getKind()
                            === SqlKind.ARRAY_VALUE_CONSTRUCTOR
                            || (expr as SqlCall).operand(0).getKind()
                            === SqlKind.MULTISET_VALUE_CONSTRUCTOR))
                newExpr = registerFrom(
                    parentScope,
                    usingScope,
                    !needAlias,
                    expr,
                    enclosingNode,
                    alias,
                    extendList,
                    forceNullable,
                    lateral
                )
                if (newExpr !== expr) {
                    call.setOperand(0, newExpr)
                }

                // If alias has a column list, introduce a namespace to translate
                // column names. We skipped registering it just now.
                if (needAlias) {
                    registerNamespace(
                        usingScope,
                        alias,
                        AliasNamespace(this, call, enclosingNode),
                        forceNullable
                    )
                }
                node
            }
            MATCH_RECOGNIZE -> {
                registerMatchRecognize(
                    parentScope, usingScope,
                    node as SqlMatchRecognize, enclosingNode, alias, forceNullable
                )
                node
            }
            PIVOT -> {
                registerPivot(
                    parentScope, usingScope, node as SqlPivot, enclosingNode,
                    alias, forceNullable
                )
                node
            }
            UNPIVOT -> {
                registerUnpivot(
                    parentScope, usingScope, node as SqlUnpivot, enclosingNode,
                    alias, forceNullable
                )
                node
            }
            TABLESAMPLE -> {
                call = node as SqlCall
                expr = call.operand(0)
                newExpr = registerFrom(
                    parentScope,
                    usingScope,
                    true,
                    expr,
                    enclosingNode,
                    alias,
                    extendList,
                    forceNullable,
                    lateral
                )
                if (newExpr !== expr) {
                    call.setOperand(0, newExpr)
                }
                node
            }
            JOIN -> {
                val join: SqlJoin = node as SqlJoin
                val joinScope = JoinScope(parentScope, usingScope, join)
                scopes.put(join, joinScope)
                val left: SqlNode = join.getLeft()
                val right: SqlNode = join.getRight()
                var forceLeftNullable = forceNullable
                var forceRightNullable = forceNullable
                when (join.getJoinType()) {
                    LEFT -> forceRightNullable = true
                    RIGHT -> forceLeftNullable = true
                    FULL -> {
                        forceLeftNullable = true
                        forceRightNullable = true
                    }
                    else -> {}
                }
                val newLeft: SqlNode = registerFrom(
                    parentScope,
                    joinScope,
                    true,
                    left,
                    left,
                    null,
                    null,
                    forceLeftNullable,
                    lateral
                )
                if (newLeft !== left) {
                    join.setLeft(newLeft)
                }
                val newRight: SqlNode = registerFrom(
                    parentScope,
                    joinScope,
                    true,
                    right,
                    right,
                    null,
                    null,
                    forceRightNullable,
                    lateral
                )
                if (newRight !== right) {
                    join.setRight(newRight)
                }
                registerSubQueries(joinScope, join.getCondition())
                val joinNamespace = JoinNamespace(this, join)
                registerNamespace(null, null, joinNamespace, forceNullable)
                join
            }
            IDENTIFIER -> {
                val id: SqlIdentifier = node as SqlIdentifier
                val newNs = IdentifierNamespace(
                    this, id, extendList, enclosingNode,
                    parentScope
                )
                registerNamespace(
                    if (register) usingScope else null, alias, newNs,
                    forceNullable
                )
                if (tableScope == null) {
                    tableScope = TableScope(parentScope, node)
                }
                tableScope.addChild(newNs, requireNonNull(alias, "alias"), forceNullable)
                if (extendList != null && extendList.size() !== 0) {
                    enclosingNode
                } else newNode
            }
            LATERAL -> registerFrom(
                parentScope,
                usingScope,
                register,
                (node as SqlCall).operand(0),
                enclosingNode,
                alias,
                extendList,
                forceNullable,
                true
            )
            COLLECTION_TABLE -> {
                call = node as SqlCall
                operand = call.operand(0)
                newOperand = registerFrom(
                    parentScope,
                    usingScope,
                    register,
                    operand,
                    enclosingNode,
                    alias,
                    extendList,
                    forceNullable, lateral
                )
                if (newOperand !== operand) {
                    call.setOperand(0, newOperand)
                }
                // If the operator is SqlWindowTableFunction, restricts the scope as
                // its first operand's (the table) scope.
                if (operand is SqlBasicCall) {
                    val call1: SqlBasicCall = operand as SqlBasicCall
                    val op: SqlOperator = call1.getOperator()
                    if (op is SqlWindowTableFunction
                        && call1.operand(0).getKind() === SqlKind.SELECT
                    ) {
                        scopes.put(node, getSelectScope(call1.operand(0)))
                        return newNode
                    }
                }
                // Put the usingScope which can be a JoinScope
                // or a SelectScope, in order to see the left items
                // of the JOIN tree.
                scopes.put(node, usingScope)
                newNode
            }
            UNNEST -> {
                if (!lateral) {
                    return registerFrom(
                        parentScope, usingScope, register, node,
                        enclosingNode, alias, extendList, forceNullable, true
                    )
                }
                if (alias == null) {
                    alias = deriveAliasNonNull(node, nextGeneratedId++)
                }
                registerQuery(
                    parentScope,
                    if (register) usingScope else null,
                    node,
                    enclosingNode,
                    alias,
                    forceNullable
                )
                newNode
            }
            SELECT, UNION, INTERSECT, EXCEPT, VALUES, WITH, OTHER_FUNCTION -> {
                if (alias == null) {
                    alias = deriveAliasNonNull(node, nextGeneratedId++)
                }
                registerQuery(
                    parentScope,
                    if (register) usingScope else null,
                    node,
                    enclosingNode,
                    alias,
                    forceNullable
                )
                newNode
            }
            OVER -> {
                if (!shouldAllowOverRelation()) {
                    throw Util.unexpected(kind)
                }
                call = node as SqlCall
                val overScope = OverScope(usingScope, call)
                scopes.put(call, overScope)
                operand = call.operand(0)
                newOperand = registerFrom(
                    parentScope,
                    overScope,
                    true,
                    operand,
                    enclosingNode,
                    alias,
                    extendList,
                    forceNullable,
                    lateral
                )
                if (newOperand !== operand) {
                    call.setOperand(0, newOperand)
                }
                for (child in overScope.children) {
                    registerNamespace(
                        if (register) usingScope else null, child.name,
                        child.namespace, forceNullable
                    )
                }
                newNode
            }
            TABLE_REF -> {
                call = node as SqlCall
                registerFrom(
                    parentScope,
                    usingScope,
                    register,
                    call.operand(0),
                    enclosingNode,
                    alias,
                    extendList,
                    forceNullable,
                    lateral
                )
                if (extendList != null && extendList.size() !== 0) {
                    enclosingNode
                } else newNode
            }
            EXTEND -> {
                val extend: SqlCall = node as SqlCall
                registerFrom(
                    parentScope,
                    usingScope,
                    true,
                    extend.getOperandList().get(0),
                    extend,
                    alias,
                    extend.getOperandList().get(1) as SqlNodeList,
                    forceNullable,
                    lateral
                )
            }
            SNAPSHOT -> {
                call = node as SqlCall
                operand = call.operand(0)
                newOperand = registerFrom(
                    parentScope,
                    usingScope,
                    register,
                    operand,
                    enclosingNode,
                    alias,
                    extendList,
                    forceNullable,
                    lateral
                )
                if (newOperand !== operand) {
                    call.setOperand(0, newOperand)
                }
                // Put the usingScope which can be a JoinScope
                // or a SelectScope, in order to see the left items
                // of the JOIN tree.
                scopes.put(node, usingScope)
                newNode
            }
            else -> throw Util.unexpected(kind)
        }
    }

    protected fun shouldAllowOverRelation(): Boolean {
        return false
    }

    /**
     * Creates a namespace for a `SELECT` node. Derived class may
     * override this factory method.
     *
     * @param select        Select node
     * @param enclosingNode Enclosing node
     * @return Select namespace
     */
    protected fun createSelectNamespace(
        select: SqlSelect?,
        enclosingNode: SqlNode?
    ): SelectNamespace {
        return SelectNamespace(this, select, enclosingNode)
    }

    /**
     * Creates a namespace for a set operation (`UNION`, `
     * INTERSECT`, or `EXCEPT`). Derived class may override
     * this factory method.
     *
     * @param call          Call to set operation
     * @param enclosingNode Enclosing node
     * @return Set operation namespace
     */
    protected fun createSetopNamespace(
        call: SqlCall?,
        enclosingNode: SqlNode?
    ): SetopNamespace {
        return SetopNamespace(this, call, enclosingNode)
    }

    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to
     * resolve objects
     * @param usingScope  Scope whose child list this scope should add itself to
     * @param node        Query node
     * @param alias       Name of this query within its parent. Must be specified
     * if usingScope != null
     */
    private fun registerQuery(
        parentScope: SqlValidatorScope,
        @Nullable usingScope: SqlValidatorScope?,
        node: SqlNode?,
        enclosingNode: SqlNode?,
        @Nullable alias: String?,
        forceNullable: Boolean
    ) {
        Preconditions.checkArgument(usingScope == null || alias != null)
        registerQuery(
            parentScope,
            usingScope,
            node,
            enclosingNode,
            alias,
            forceNullable,
            true
        )
    }

    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to
     * resolve objects
     * @param usingScope  Scope whose child list this scope should add itself to
     * @param node        Query node
     * @param alias       Name of this query within its parent. Must be specified
     * if usingScope != null
     * @param checkUpdate if true, validate that the update feature is supported
     * if validating the update statement
     */
    private fun registerQuery(
        parentScope: SqlValidatorScope,
        @Nullable usingScope: SqlValidatorScope?,
        node: SqlNode?,
        enclosingNode: SqlNode?,
        @Nullable alias: String?,
        forceNullable: Boolean,
        checkUpdate: Boolean
    ) {
        requireNonNull(node, "node")
        requireNonNull(enclosingNode, "enclosingNode")
        Preconditions.checkArgument(usingScope == null || alias != null)
        val call: SqlCall?
        val operands: List<SqlNode>
        when (node.getKind()) {
            SELECT -> {
                val select: SqlSelect? = node as SqlSelect?
                val selectNs: SelectNamespace = createSelectNamespace(select, enclosingNode)
                registerNamespace(usingScope, alias, selectNs, forceNullable)
                val windowParentScope: SqlValidatorScope = if (usingScope != null) usingScope else parentScope
                val selectScope = SelectScope(parentScope, windowParentScope, select)
                scopes.put(select, selectScope)

                // Start by registering the WHERE clause
                clauseScopes.put(IdPair.of(select, Clause.WHERE), selectScope)
                registerOperandSubQueries(
                    selectScope,
                    select,
                    SqlSelect.WHERE_OPERAND
                )

                // Register FROM with the inherited scope 'parentScope', not
                // 'selectScope', otherwise tables in the FROM clause would be
                // able to see each other.
                val from: SqlNode = select.getFrom()
                if (from != null) {
                    val newFrom: SqlNode = registerFrom(
                        parentScope,
                        selectScope,
                        true,
                        from,
                        from,
                        null,
                        null,
                        false,
                        false
                    )
                    if (newFrom !== from) {
                        select.setFrom(newFrom)
                    }
                }

                // If this is an aggregating query, the SELECT list and HAVING
                // clause use a different scope, where you can only reference
                // columns which are in the GROUP BY clause.
                var aggScope: SqlValidatorScope = selectScope
                if (isAggregate(select)) {
                    aggScope = AggregatingSelectScope(selectScope, select, false)
                    clauseScopes.put(IdPair.of(select, Clause.SELECT), aggScope)
                } else {
                    clauseScopes.put(IdPair.of(select, Clause.SELECT), selectScope)
                }
                if (select.getGroup() != null) {
                    val groupByScope = GroupByScope(selectScope, select.getGroup(), select)
                    clauseScopes.put(IdPair.of(select, Clause.GROUP_BY), groupByScope)
                    registerSubQueries(groupByScope, select.getGroup())
                }
                registerOperandSubQueries(
                    aggScope,
                    select,
                    SqlSelect.HAVING_OPERAND
                )
                registerSubQueries(aggScope, SqlNonNullableAccessors.getSelectList(select))
                val orderList: SqlNodeList = select.getOrderList()
                if (orderList != null) {
                    // If the query is 'SELECT DISTINCT', restrict the columns
                    // available to the ORDER BY clause.
                    if (select.isDistinct()) {
                        aggScope = AggregatingSelectScope(selectScope, select, true)
                    }
                    val orderScope = OrderByScope(aggScope, orderList, select)
                    clauseScopes.put(IdPair.of(select, Clause.ORDER), orderScope)
                    registerSubQueries(orderScope, orderList)
                    if (!isAggregate(select)) {
                        // Since this is not an aggregating query,
                        // there cannot be any aggregates in the ORDER BY clause.
                        val agg: SqlNode = aggFinder.findAgg(orderList)
                        if (agg != null) {
                            throw newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy())
                        }
                    }
                }
            }
            INTERSECT -> {
                validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition())
                registerSetop(
                    parentScope,
                    usingScope,
                    node,
                    node,
                    alias,
                    forceNullable
                )
            }
            EXCEPT -> {
                validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition())
                registerSetop(
                    parentScope,
                    usingScope,
                    node,
                    node,
                    alias,
                    forceNullable
                )
            }
            UNION -> registerSetop(
                parentScope,
                usingScope,
                node,
                node,
                alias,
                forceNullable
            )
            WITH -> registerWith(
                parentScope, usingScope, node as SqlWith?, enclosingNode,
                alias, forceNullable, checkUpdate
            )
            VALUES -> {
                call = node as SqlCall?
                scopes.put(call, parentScope)
                val tableConstructorNamespace = TableConstructorNamespace(
                    this,
                    call,
                    parentScope,
                    enclosingNode
                )
                registerNamespace(
                    usingScope,
                    alias,
                    tableConstructorNamespace,
                    forceNullable
                )
                operands = call.getOperandList()
                var i = 0
                while (i < operands.size()) {
                    assert(operands[i].getKind() === SqlKind.ROW)

                    // FIXME jvs 9-Feb-2005:  Correlation should
                    // be illegal in these sub-queries.  Same goes for
                    // any non-lateral SELECT in the FROM list.
                    registerOperandSubQueries(parentScope, call, i)
                    ++i
                }
            }
            INSERT -> {
                val insertCall: SqlInsert? = node as SqlInsert?
                val insertNs = InsertNamespace(
                    this,
                    insertCall,
                    enclosingNode,
                    parentScope
                )
                registerNamespace(usingScope, null, insertNs, forceNullable)
                registerQuery(
                    parentScope,
                    usingScope,
                    insertCall.getSource(),
                    enclosingNode,
                    null,
                    false
                )
            }
            DELETE -> {
                val deleteCall: SqlDelete? = node as SqlDelete?
                val deleteNs = DeleteNamespace(
                    this,
                    deleteCall,
                    enclosingNode,
                    parentScope
                )
                registerNamespace(usingScope, null, deleteNs, forceNullable)
                registerQuery(
                    parentScope,
                    usingScope,
                    SqlNonNullableAccessors.getSourceSelect(deleteCall),
                    enclosingNode,
                    null,
                    false
                )
            }
            UPDATE -> {
                if (checkUpdate) {
                    validateFeature(
                        RESOURCE.sQLFeature_E101_03(),
                        node.getParserPosition()
                    )
                }
                val updateCall: SqlUpdate? = node as SqlUpdate?
                val updateNs = UpdateNamespace(
                    this,
                    updateCall,
                    enclosingNode,
                    parentScope
                )
                registerNamespace(usingScope, null, updateNs, forceNullable)
                registerQuery(
                    parentScope,
                    usingScope,
                    SqlNonNullableAccessors.getSourceSelect(updateCall),
                    enclosingNode,
                    null,
                    false
                )
            }
            MERGE -> {
                validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition())
                val mergeCall: SqlMerge? = node as SqlMerge?
                val mergeNs = MergeNamespace(
                    this,
                    mergeCall,
                    enclosingNode,
                    parentScope
                )
                registerNamespace(usingScope, null, mergeNs, forceNullable)
                registerQuery(
                    parentScope,
                    usingScope,
                    SqlNonNullableAccessors.getSourceSelect(mergeCall),
                    enclosingNode,
                    null,
                    false
                )

                // update call can reference either the source table reference
                // or the target table, so set its parent scope to the merge's
                // source select; when validating the update, skip the feature
                // validation check
                val mergeUpdateCall: SqlUpdate = mergeCall.getUpdateCall()
                if (mergeUpdateCall != null) {
                    registerQuery(
                        getScope(SqlNonNullableAccessors.getSourceSelect(mergeCall), Clause.WHERE),
                        null,
                        mergeUpdateCall,
                        enclosingNode,
                        null,
                        false,
                        false
                    )
                }
                val mergeInsertCall: SqlInsert = mergeCall.getInsertCall()
                if (mergeInsertCall != null) {
                    registerQuery(
                        parentScope,
                        null,
                        mergeInsertCall,
                        enclosingNode,
                        null,
                        false
                    )
                }
            }
            UNNEST -> {
                call = node as SqlCall?
                val unnestNs = UnnestNamespace(this, call, parentScope, enclosingNode)
                registerNamespace(
                    usingScope,
                    alias,
                    unnestNs,
                    forceNullable
                )
                registerOperandSubQueries(parentScope, call, 0)
                scopes.put(node, parentScope)
            }
            OTHER_FUNCTION -> {
                call = node as SqlCall?
                val procNs = ProcedureNamespace(
                    this,
                    parentScope,
                    call,
                    enclosingNode
                )
                registerNamespace(
                    usingScope,
                    alias,
                    procNs,
                    forceNullable
                )
                registerSubQueries(parentScope, call)
            }
            MULTISET_QUERY_CONSTRUCTOR, MULTISET_VALUE_CONSTRUCTOR -> {
                validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition())
                call = node as SqlCall?
                val cs = CollectScope(parentScope, usingScope, call)
                val tableConstructorNs = CollectNamespace(call, cs, enclosingNode)
                val alias2 = deriveAliasNonNull(node, nextGeneratedId++)
                registerNamespace(
                    usingScope,
                    alias2,
                    tableConstructorNs,
                    forceNullable
                )
                operands = call.getOperandList()
                var i = 0
                while (i < operands.size()) {
                    registerOperandSubQueries(parentScope, call, i)
                    i++
                }
            }
            else -> throw Util.unexpected(node.getKind())
        }
    }

    private fun registerSetop(
        parentScope: SqlValidatorScope,
        @Nullable usingScope: SqlValidatorScope?,
        node: SqlNode?,
        enclosingNode: SqlNode?,
        @Nullable alias: String?,
        forceNullable: Boolean
    ) {
        val call: SqlCall? = node as SqlCall?
        val setopNamespace: SetopNamespace = createSetopNamespace(call, enclosingNode)
        registerNamespace(usingScope, alias, setopNamespace, forceNullable)

        // A setop is in the same scope as its parent.
        scopes.put(call, parentScope)
        for (operand in call.getOperandList()) {
            registerQuery(
                parentScope,
                null,
                operand,
                operand,
                null,
                false
            )
        }
    }

    private fun registerWith(
        parentScope: SqlValidatorScope,
        @Nullable usingScope: SqlValidatorScope?,
        with: SqlWith?,
        enclosingNode: SqlNode?,
        @Nullable alias: String?,
        forceNullable: Boolean,
        checkUpdate: Boolean
    ) {
        val withNamespace = WithNamespace(this, with, enclosingNode)
        registerNamespace(usingScope, alias, withNamespace, forceNullable)
        var scope: SqlValidatorScope = parentScope
        for (withItem_ in with.withList) {
            val withItem: SqlWithItem = withItem_ as SqlWithItem
            val withScope = WithScope(scope, withItem)
            scopes.put(withItem, withScope)
            registerQuery(
                scope, null, withItem.query, with,
                withItem.name.getSimple(), false
            )
            registerNamespace(
                null, alias,
                WithItemNamespace(this, withItem, enclosingNode),
                false
            )
            scope = withScope
        }
        registerQuery(
            scope, null, with.body, enclosingNode, alias, forceNullable,
            checkUpdate
        )
    }

    @Override
    override fun isAggregate(select: SqlSelect): Boolean {
        if (getAggregate(select) != null) {
            return true
        }
        // Also when nested window aggregates are present
        for (call in overFinder.findAll(SqlNonNullableAccessors.getSelectList(select))) {
            assert(call.getKind() === SqlKind.OVER)
            if (isNestedAggregateWindow(call.operand(0))) {
                return true
            }
            if (isOverAggregateWindow(call.operand(1))) {
                return true
            }
        }
        return false
    }

    protected fun isNestedAggregateWindow(node: SqlNode?): Boolean {
        val nestedAggFinder = AggFinder(
            opTab, false, false, false, aggFinder,
            catalogReader.nameMatcher()
        )
        return nestedAggFinder.findAgg(node) != null
    }

    protected fun isOverAggregateWindow(node: SqlNode?): Boolean {
        return aggFinder.findAgg(node) != null
    }

    /** Returns the parse tree node (GROUP BY, HAVING, or an aggregate function
     * call) that causes `select` to be an aggregate query, or null if it
     * is not an aggregate query.
     *
     *
     * The node is useful context for error messages,
     * but you cannot assume that the node is the only aggregate function.  */
    @Nullable
    protected fun getAggregate(select: SqlSelect): SqlNode? {
        var node: SqlNode = select.getGroup()
        if (node != null) {
            return node
        }
        node = select.getHaving()
        return if (node != null) {
            node
        } else getAgg(select)
    }

    /** If there is at least one call to an aggregate function, returns the
     * first.  */
    @Nullable
    private fun getAgg(select: SqlSelect): SqlNode {
        val selectScope: SelectScope? = getRawSelectScope(select)
        if (selectScope != null) {
            val selectList: List<SqlNode> = selectScope.getExpandedSelectList()
            if (selectList != null) {
                return aggFinder.findAgg(selectList)
            }
        }
        return aggFinder.findAgg(SqlNonNullableAccessors.getSelectList(select))
    }

    @Deprecated
    @Override
    override fun isAggregate(selectNode: SqlNode?): Boolean {
        return aggFinder.findAgg(selectNode) != null
    }

    private fun validateNodeFeature(node: SqlNode) {
        when (node.getKind()) {
            MULTISET_VALUE_CONSTRUCTOR -> validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition())
            else -> {}
        }
    }

    private fun registerSubQueries(
        parentScope: SqlValidatorScope,
        @Nullable node: SqlNode?
    ) {
        if (node == null) {
            return
        }
        if (node.getKind().belongsTo(SqlKind.QUERY)
            || node.getKind() === SqlKind.MULTISET_QUERY_CONSTRUCTOR || node.getKind() === SqlKind.MULTISET_VALUE_CONSTRUCTOR
        ) {
            registerQuery(parentScope, null, node, node, null, false)
        } else if (node is SqlCall) {
            validateNodeFeature(node)
            val call: SqlCall = node as SqlCall
            for (i in 0 until call.operandCount()) {
                registerOperandSubQueries(parentScope, call, i)
            }
        } else if (node is SqlNodeList) {
            val list: SqlNodeList = node as SqlNodeList
            var i = 0
            val count: Int = list.size()
            while (i < count) {
                var listNode: SqlNode = list.get(i)
                if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
                    listNode = SqlStdOperatorTable.SCALAR_QUERY.createCall(
                        listNode.getParserPosition(),
                        listNode
                    )
                    list.set(i, listNode)
                }
                registerSubQueries(parentScope, listNode)
                i++
            }
        } else {
            // atomic node -- can be ignored
        }
    }

    /**
     * Registers any sub-queries inside a given call operand, and converts the
     * operand to a scalar sub-query if the operator requires it.
     *
     * @param parentScope    Parent scope
     * @param call           Call
     * @param operandOrdinal Ordinal of operand within call
     * @see SqlOperator.argumentMustBeScalar
     */
    private fun registerOperandSubQueries(
        parentScope: SqlValidatorScope,
        call: SqlCall?,
        operandOrdinal: Int
    ) {
        var operand: SqlNode = call.operand(operandOrdinal) ?: return
        if (operand.getKind().belongsTo(SqlKind.QUERY)
            && call.getOperator().argumentMustBeScalar(operandOrdinal)
        ) {
            operand = SqlStdOperatorTable.SCALAR_QUERY.createCall(
                operand.getParserPosition(),
                operand
            )
            call.setOperand(operandOrdinal, operand)
        }
        registerSubQueries(parentScope, operand)
    }

    @Override
    fun validateIdentifier(id: SqlIdentifier, scope: SqlValidatorScope) {
        val fqId: SqlQualified = scope.fullyQualify(id)
        if (config.columnReferenceExpansion()) {
            // NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its
            // own ideas about qualification.
            id.assignNamesFrom(fqId.identifier)
        } else {
            Util.discard(fqId)
        }
    }

    @Override
    override fun validateLiteral(literal: SqlLiteral) {
        when (literal.getTypeName()) {
            DECIMAL -> {
                // Decimal and long have the same precision (as 64-bit integers), so
                // the unscaled value of a decimal must fit into a long.

                // REVIEW jvs 4-Aug-2004:  This should probably be calling over to
                // the available calculator implementations to see what they
                // support.  For now use ESP instead.
                //
                // jhyde 2006/12/21: I think the limits should be baked into the
                // type system, not dependent on the calculator implementation.
                val bd: BigDecimal = literal.getValueAs(BigDecimal::class.java)
                val unscaled: BigInteger = bd.unscaledValue()
                val longValue: Long = unscaled.longValue()
                if (!BigInteger.valueOf(longValue).equals(unscaled)) {
                    // overflow
                    throw newValidationError(
                        literal,
                        RESOURCE.numberLiteralOutOfRange(bd.toString())
                    )
                }
            }
            DOUBLE -> validateLiteralAsDouble(literal)
            BINARY -> {
                val bitString: BitString = literal.getValueAs(BitString::class.java)
                if (bitString.getBitCount() % 8 !== 0) {
                    throw newValidationError(literal, RESOURCE.binaryLiteralOdd())
                }
            }
            DATE, TIME, TIMESTAMP -> {
                val calendar: Calendar = literal.getValueAs(Calendar::class.java)
                val year: Int = calendar.get(Calendar.YEAR)
                val era: Int = calendar.get(Calendar.ERA)
                if (year < 1 || era == GregorianCalendar.BC || year > 9999) {
                    throw newValidationError(
                        literal,
                        RESOURCE.dateLiteralOutOfRange(literal.toString())
                    )
                }
            }
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> if (literal is SqlIntervalLiteral) {
                val interval: SqlIntervalLiteral.IntervalValue =
                    literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
                val intervalQualifier: SqlIntervalQualifier = interval.getIntervalQualifier()

                // ensure qualifier is good before attempting to validate literal
                validateIntervalQualifier(intervalQualifier)
                val intervalStr: String = interval.getIntervalLiteral()
                // throws CalciteContextException if string is invalid
                val values: IntArray = intervalQualifier.evaluateIntervalLiteral(
                    intervalStr,
                    literal.getParserPosition(), typeFactory.getTypeSystem()
                )
                Util.discard(values)
            }
            else -> {}
        }
    }

    private fun validateLiteralAsDouble(literal: SqlLiteral) {
        val bd: BigDecimal = literal.getValueAs(BigDecimal::class.java)
        val d: Double = bd.doubleValue()
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            // overflow
            throw newValidationError(
                literal,
                RESOURCE.numberLiteralOutOfRange(Util.toScientificNotation(bd))
            )
        }

        // REVIEW jvs 4-Aug-2004:  what about underflow?
    }

    @Override
    override fun validateIntervalQualifier(qualifier: SqlIntervalQualifier?) {
        assert(qualifier != null)
        var startPrecisionOutOfRange = false
        var fractionalSecondPrecisionOutOfRange = false
        val typeSystem: RelDataTypeSystem = typeFactory.getTypeSystem()
        val startPrecision: Int = qualifier.getStartPrecision(typeSystem)
        val fracPrecision: Int = qualifier.getFractionalSecondPrecision(typeSystem)
        val maxPrecision: Int = typeSystem.getMaxPrecision(qualifier.typeName())
        val minPrecision: Int = qualifier.typeName().getMinPrecision()
        val minScale: Int = qualifier.typeName().getMinScale()
        val maxScale: Int = typeSystem.getMaxScale(qualifier.typeName())
        if (startPrecision < minPrecision || startPrecision > maxPrecision) {
            startPrecisionOutOfRange = true
        } else {
            if (fracPrecision < minScale || fracPrecision > maxScale) {
                fractionalSecondPrecisionOutOfRange = true
            }
        }
        if (startPrecisionOutOfRange) {
            throw newValidationError(
                qualifier,
                RESOURCE.intervalStartPrecisionOutOfRange(
                    startPrecision,
                    "INTERVAL $qualifier"
                )
            )
        } else if (fractionalSecondPrecisionOutOfRange) {
            throw newValidationError(
                qualifier,
                RESOURCE.intervalFractionalSecondPrecisionOutOfRange(
                    fracPrecision,
                    "INTERVAL $qualifier"
                )
            )
        }
    }

    /**
     * Validates the FROM clause of a query, or (recursively) a child node of
     * the FROM clause: AS, OVER, JOIN, VALUES, or sub-query.
     *
     * @param node          Node in FROM clause, typically a table or derived
     * table
     * @param targetRowType Desired row type of this expression, or
     * [.unknownType] if not fussy. Must not be null.
     * @param scope         Scope
     */
    protected fun validateFrom(
        node: SqlNode,
        targetRowType: RelDataType,
        scope: SqlValidatorScope
    ) {
        requireNonNull(targetRowType, "targetRowType")
        when (node.getKind()) {
            AS, TABLE_REF -> validateFrom(
                (node as SqlCall).operand(0),
                targetRowType,
                scope
            )
            VALUES -> validateValues(node as SqlCall, targetRowType, scope)
            JOIN -> validateJoin(node as SqlJoin, scope)
            OVER -> validateOver(node as SqlCall, scope)
            UNNEST -> validateUnnest(node as SqlCall, scope, targetRowType)
            else -> validateQuery(node, scope, targetRowType)
        }

        // Validate the namespace representation of the node, just in case the
        // validation did not occur implicitly.
        getNamespaceOrThrow(node, scope).validate(targetRowType)
    }

    protected fun validateOver(call: SqlCall?, scope: SqlValidatorScope?) {
        throw AssertionError("OVER unexpected in this context")
    }

    protected fun validateUnnest(call: SqlCall, scope: SqlValidatorScope, targetRowType: RelDataType?) {
        for (i in 0 until call.operandCount()) {
            val expandedItem: SqlNode = expand(call.operand(i), scope)
            call.setOperand(i, expandedItem)
        }
        validateQuery(call, scope, targetRowType)
    }

    private fun checkRollUpInUsing(
        identifier: SqlIdentifier,
        leftOrRight: SqlNode, scope: SqlValidatorScope
    ) {
        val namespace: SqlValidatorNamespace = getNamespace(leftOrRight, scope)
        if (namespace != null) {
            val sqlValidatorTable: SqlValidatorTable = namespace.getTable()
            if (sqlValidatorTable != null) {
                val table: Table = sqlValidatorTable.table()
                val column: String = Util.last(identifier.names)
                if (table.isRolledUp(column)) {
                    throw newValidationError(
                        identifier,
                        RESOURCE.rolledUpNotAllowed(column, "USING")
                    )
                }
            }
        }
    }

    protected fun validateJoin(join: SqlJoin, scope: SqlValidatorScope) {
        val left: SqlNode = join.getLeft()
        val right: SqlNode = join.getRight()
        var condition: SqlNode? = join.getCondition()
        val natural: Boolean = join.isNatural()
        val joinType: JoinType = join.getJoinType()
        val conditionType: JoinConditionType = join.getConditionType()
        val joinScope: SqlValidatorScope = getScopeOrThrow(join) // getJoinScope?
        validateFrom(left, unknownType, joinScope)
        validateFrom(right, unknownType, joinScope)
        when (conditionType) {
            NONE -> Preconditions.checkArgument(condition == null)
            ON -> {
                requireNonNull(condition, "join.getCondition()")
                val expandedCondition: SqlNode = expand(condition, joinScope)
                join.setOperand(5, expandedCondition)
                condition = getCondition(join)
                validateWhereOrOn(joinScope, condition, "ON")
                checkRollUp(null, join, condition, joinScope, "ON")
            }
            USING -> {
                val list: SqlNodeList = requireNonNull(condition, "join.getCondition()") as SqlNodeList

                // Parser ensures that using clause is not empty.
                Preconditions.checkArgument(list.size() > 0, "Empty USING clause")
                for (node in list) {
                    val id: SqlIdentifier = node as SqlIdentifier
                    val leftColType: RelDataType = validateUsingCol(id, left)
                    val rightColType: RelDataType = validateUsingCol(id, right)
                    if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
                        throw newValidationError(
                            id,
                            RESOURCE.naturalOrUsingColumnNotCompatible(
                                id.getSimple(),
                                leftColType.toString(), rightColType.toString()
                            )
                        )
                    }
                    checkRollUpInUsing(id, left, scope)
                    checkRollUpInUsing(id, right, scope)
                }
            }
            else -> throw Util.unexpected(conditionType)
        }

        // Validate NATURAL.
        if (natural) {
            if (condition != null) {
                throw newValidationError(
                    condition,
                    RESOURCE.naturalDisallowsOnOrUsing()
                )
            }

            // Join on fields that occur exactly once on each side. Ignore
            // fields that occur more than once on either side.
            val leftRowType: RelDataType = getNamespaceOrThrow(left).getRowType()
            val rightRowType: RelDataType = getNamespaceOrThrow(right).getRowType()
            val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
            val naturalColumnNames: List<String> = SqlValidatorUtil.deriveNaturalJoinColumnList(
                nameMatcher,
                leftRowType, rightRowType
            )

            // Check compatibility of the chosen columns.
            for (name in naturalColumnNames) {
                val leftColType: RelDataType = requireNonNull(
                    nameMatcher.field(leftRowType, name)
                ) { "unable to find left field $name in $leftRowType" }.getType()
                val rightColType: RelDataType = requireNonNull(
                    nameMatcher.field(rightRowType, name)
                ) { "unable to find right field $name in $rightRowType" }.getType()
                if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
                    throw newValidationError(
                        join,
                        RESOURCE.naturalOrUsingColumnNotCompatible(
                            name,
                            leftColType.toString(), rightColType.toString()
                        )
                    )
                }
            }
        }
        when (joinType) {
            LEFT_SEMI_JOIN -> {
                if (!config.sqlConformance().isLiberal()) {
                    throw newValidationError(
                        join.getJoinTypeNode(),
                        RESOURCE.dialectDoesNotSupportFeature("LEFT SEMI JOIN")
                    )
                }
                if (condition == null && !natural) {
                    throw newValidationError(join, RESOURCE.joinRequiresCondition())
                }
            }
            INNER, LEFT, RIGHT, FULL -> if (condition == null && !natural) {
                throw newValidationError(join, RESOURCE.joinRequiresCondition())
            }
            COMMA, CROSS -> {
                if (condition != null) {
                    throw newValidationError(
                        join.getConditionTypeNode(),
                        RESOURCE.crossJoinDisallowsCondition()
                    )
                }
                if (natural) {
                    throw newValidationError(
                        join.getConditionTypeNode(),
                        RESOURCE.crossJoinDisallowsCondition()
                    )
                }
            }
            else -> throw Util.unexpected(joinType)
        }
    }

    /**
     * Throws an error if there is an aggregate or windowed aggregate in the
     * given clause.
     *
     * @param aggFinder Finder for the particular kind(s) of aggregate function
     * @param node      Parse tree
     * @param clause    Name of clause: "WHERE", "GROUP BY", "ON"
     */
    private fun validateNoAggs(
        aggFinder: AggFinder, node: SqlNode?,
        clause: String
    ) {
        val agg: SqlCall = aggFinder.findAgg(node) ?: return
        val op: SqlOperator = agg.getOperator()
        if (op === SqlStdOperatorTable.OVER) {
            throw newValidationError(
                agg,
                RESOURCE.windowedAggregateIllegalInClause(clause)
            )
        } else if (op.isGroup() || op.isGroupAuxiliary()) {
            throw newValidationError(
                agg,
                RESOURCE.groupFunctionMustAppearInGroupByClause(op.getName())
            )
        } else {
            throw newValidationError(
                agg,
                RESOURCE.aggregateIllegalInClause(clause)
            )
        }
    }

    private fun validateUsingCol(id: SqlIdentifier, leftOrRight: SqlNode): RelDataType {
        if (id.names.size() === 1) {
            val name: String = id.names.get(0)
            val namespace: SqlValidatorNamespace = getNamespaceOrThrow(leftOrRight)
            val rowType: RelDataType = namespace.getRowType()
            val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
            val field: RelDataTypeField = nameMatcher.field(rowType, name)
            if (field != null) {
                if (nameMatcher.frequency(rowType.getFieldNames(), name) > 1) {
                    throw newValidationError(
                        id,
                        RESOURCE.columnInUsingNotUnique(id.toString())
                    )
                }
                return field.getType()
            }
        }
        throw newValidationError(id, RESOURCE.columnNotFound(id.toString()))
    }

    /**
     * Validates a SELECT statement.
     *
     * @param select        Select statement
     * @param targetRowType Desired row type, must not be null, may be the data
     * type 'unknown'.
     */
    protected fun validateSelect(
        select: SqlSelect,
        targetRowType: RelDataType?
    ) {
        assert(targetRowType != null)
        // Namespace is either a select namespace or a wrapper around one.
        val ns: SelectNamespace = getNamespaceOrThrow(select).unwrap(SelectNamespace::class.java)
        assert(ns.rowType == null)
        val distinctNode: SqlNode = select.getModifierNode(SqlSelectKeyword.DISTINCT)
        if (distinctNode != null) {
            validateFeature(
                RESOURCE.sQLFeature_E051_01(),
                distinctNode
                    .getParserPosition()
            )
        }
        val selectItems: SqlNodeList = SqlNonNullableAccessors.getSelectList(select)
        var fromType: RelDataType? = unknownType
        if (selectItems.size() === 1) {
            val selectItem: SqlNode = selectItems.get(0)
            if (selectItem is SqlIdentifier) {
                val id: SqlIdentifier = selectItem as SqlIdentifier
                if (id.isStar() && id.names.size() === 1) {
                    // Special case: for INSERT ... VALUES(?,?), the SQL
                    // standard says we're supposed to propagate the target
                    // types down.  So iff the select list is an unqualified
                    // star (as it will be after an INSERT ... VALUES has been
                    // expanded), then propagate.
                    fromType = targetRowType
                }
            }
        }

        // Make sure that items in FROM clause have distinct aliases.
        val fromScope: SelectScope = requireNonNull(
            getFromScope(select)
        ) { "fromScope for $select" } as SelectScope
        var names: List<String?> = fromScope.getChildNames()
        if (!catalogReader.nameMatcher().isCaseSensitive()) {
            names = names.stream()
                .< String > map < String ? > { s -> if (s == null) null else s.toUpperCase(Locale.ROOT) }
                .collect(Collectors.toList())
        }
        val duplicateAliasOrdinal: Int = Util.firstDuplicate(names)
        if (duplicateAliasOrdinal >= 0) {
            val child: ScopeChild = fromScope.children.get(duplicateAliasOrdinal)
            throw newValidationError(
                requireNonNull(
                    child.namespace.getEnclosingNode()
                ) { "enclosingNode of namespace of " + child.name },
                RESOURCE.fromAliasDuplicate(child.name)
            )
        }
        if (select.getFrom() == null) {
            if (config.sqlConformance().isFromRequired()) {
                throw newValidationError(select, RESOURCE.selectMissingFrom())
            }
        } else {
            validateFrom(select.getFrom(), fromType, fromScope)
        }
        validateWhereClause(select)
        validateGroupClause(select)
        validateHavingClause(select)
        validateWindowClause(select)
        handleOffsetFetch(select.getOffset(), select.getFetch())

        // Validate the SELECT clause late, because a select item might
        // depend on the GROUP BY list, or the window function might reference
        // window name in the WINDOW clause etc.
        val rowType: RelDataType = validateSelectList(selectItems, select, targetRowType)
        ns.setType(rowType)

        // Validate ORDER BY after we have set ns.rowType because in some
        // dialects you can refer to columns of the select list, e.g.
        // "SELECT empno AS x FROM emp ORDER BY x"
        validateOrderList(select)
        if (shouldCheckForRollUp(select.getFrom())) {
            checkRollUpInSelectList(select)
            checkRollUp(null, select, select.getWhere(), getWhereScope(select))
            checkRollUp(null, select, select.getHaving(), getHavingScope(select))
            checkRollUpInWindowDecl(select)
            checkRollUpInGroupBy(select)
            checkRollUpInOrderBy(select)
        }
    }

    private fun checkRollUpInSelectList(select: SqlSelect) {
        val scope: SqlValidatorScope = getSelectScope(select)
        for (item in SqlNonNullableAccessors.getSelectList(select)) {
            checkRollUp(null, select, item, scope)
        }
    }

    private fun checkRollUpInGroupBy(select: SqlSelect) {
        val group: SqlNodeList = select.getGroup()
        if (group != null) {
            for (node in group) {
                checkRollUp(null, select, node, getGroupScope(select), "GROUP BY")
            }
        }
    }

    private fun checkRollUpInOrderBy(select: SqlSelect) {
        val orderList: SqlNodeList = select.getOrderList()
        if (orderList != null) {
            for (node in orderList) {
                checkRollUp(null, select, node, getOrderScope(select), "ORDER BY")
            }
        }
    }

    private fun checkRollUpInWindow(@Nullable window: SqlWindow?, scope: SqlValidatorScope) {
        if (window != null) {
            for (node in window.getPartitionList()) {
                checkRollUp(null, window, node, scope, "PARTITION BY")
            }
            for (node in window.getOrderList()) {
                checkRollUp(null, window, node, scope, "ORDER BY")
            }
        }
    }

    private fun checkRollUpInWindowDecl(select: SqlSelect) {
        for (decl in select.getWindowList()) {
            checkRollUpInWindow(decl as SqlWindow, getSelectScope(select))
        }
    }

    private fun checkRollUp(
        @Nullable grandParent: SqlNode?, @Nullable parent: SqlNode?,
        @Nullable current: SqlNode?, scope: SqlValidatorScope, @Nullable optionalClause: String?
    ) {
        var current: SqlNode? = current
        current = stripAs(current)
        if (current is SqlCall && current !is SqlSelect) {
            // Validate OVER separately
            checkRollUpInWindow(getWindowInOver(current), scope)
            current = stripOver(current)
            val stripDot: SqlNode = requireNonNull(stripDot(current), "stripDot(current)")
            val children: List<SqlNode?> = (stripAs(stripDot) as SqlCall).getOperandList()
            for (child in children) {
                checkRollUp(parent, current, child, scope, optionalClause)
            }
        } else if (current is SqlIdentifier) {
            val id: SqlIdentifier? = current as SqlIdentifier?
            if (!id.isStar() && isRolledUpColumn(id, scope)) {
                if (!isAggregation(requireNonNull(parent, "parent").getKind())
                    || !isRolledUpColumnAllowedInAgg(id, scope, parent as SqlCall?, grandParent)
                ) {
                    val context = optionalClause ?: parent.getKind().toString()
                    throw newValidationError(
                        id,
                        RESOURCE.rolledUpNotAllowed(deriveAliasNonNull(id, 0), context)
                    )
                }
            }
        }
    }

    private fun checkRollUp(
        @Nullable grandParent: SqlNode?, parent: SqlNode,
        @Nullable current: SqlNode, scope: SqlValidatorScope
    ) {
        checkRollUp(grandParent, parent, current, scope, null)
    }

    @Nullable
    private fun findTableColumnPair(
        identifier: SqlIdentifier?,
        scope: SqlValidatorScope
    ): Pair<String, String>? {
        val call: SqlCall? = makeNullaryCall(identifier)
        if (call != null) {
            return null
        }
        val qualified: SqlQualified = scope.fullyQualify(identifier)
        val names: List<String> = qualified.identifier.names
        return if (names.size() < 2) {
            null
        } else Pair(names[names.size() - 2], Util.last(names))
    }

    // Returns true iff the given column is valid inside the given aggCall.
    private fun isRolledUpColumnAllowedInAgg(
        identifier: SqlIdentifier?, scope: SqlValidatorScope,
        aggCall: SqlCall?, @Nullable parent: SqlNode?
    ): Boolean {
        val pair: Pair<String, String> = findTableColumnPair(identifier, scope) ?: return true
        val columnName: String = pair.right
        val table: Table? = resolveTable(identifier, scope)
        return if (table != null) {
            table.rolledUpColumnValidInsideAgg(
                columnName, aggCall, parent,
                catalogReader.getConfig()
            )
        } else true
    }

    // Returns true iff the given column is actually rolled up.
    private fun isRolledUpColumn(identifier: SqlIdentifier?, scope: SqlValidatorScope): Boolean {
        val pair: Pair<String, String> = findTableColumnPair(identifier, scope) ?: return false
        val columnName: String = pair.right
        val table: Table? = resolveTable(identifier, scope)
        return if (table != null) {
            table.isRolledUp(columnName)
        } else false
    }

    /** Validates that a query can deliver the modality it promises. Only called
     * on the top-most SELECT or set operator in the tree.  */
    private fun validateModality(query: SqlNode) {
        val modality: SqlModality = deduceModality(query)
        if (query is SqlSelect) {
            val select: SqlSelect = query as SqlSelect
            validateModality(select, modality, true)
        } else if (query.getKind() === SqlKind.VALUES) {
            when (modality) {
                STREAM -> throw newValidationError(query, Static.RESOURCE.cannotStreamValues())
                else -> {}
            }
        } else {
            assert(query.isA(SqlKind.SET_QUERY))
            val call: SqlCall = query as SqlCall
            for (operand in call.getOperandList()) {
                if (deduceModality(operand) !== modality) {
                    throw newValidationError(
                        operand,
                        Static.RESOURCE.streamSetOpInconsistentInputs()
                    )
                }
                validateModality(operand)
            }
        }
    }

    @Override
    override fun validateModality(
        select: SqlSelect, modality: SqlModality?,
        fail: Boolean
    ): Boolean {
        val scope: SelectScope = getRawSelectScopeNonNull(select)
        when (modality) {
            STREAM -> if (scope.children.size() === 1) {
                for (child in scope.children) {
                    if (!child.namespace.supportsModality(modality)) {
                        return if (fail) {
                            val node: SqlNode = SqlNonNullableAccessors.getNode(child)
                            throw newValidationError(
                                node,
                                Static.RESOURCE.cannotConvertToStream(child.name)
                            )
                        } else {
                            false
                        }
                    }
                }
            } else {
                var supportsModalityCount = 0
                for (child in scope.children) {
                    if (child.namespace.supportsModality(modality)) {
                        ++supportsModalityCount
                    }
                }
                if (supportsModalityCount == 0) {
                    return if (fail) {
                        val inputs: String = String.join(", ", scope.getChildNames())
                        throw newValidationError(
                            select,
                            Static.RESOURCE.cannotStreamResultsForNonStreamingInputs(inputs)
                        )
                    } else {
                        false
                    }
                }
            }
            else -> for (child in scope.children) {
                if (!child.namespace.supportsModality(modality)) {
                    return if (fail) {
                        val node: SqlNode = SqlNonNullableAccessors.getNode(child)
                        throw newValidationError(
                            node,
                            Static.RESOURCE.cannotConvertToRelation(child.name)
                        )
                    } else {
                        false
                    }
                }
            }
        }

        // Make sure that aggregation is possible.
        val aggregateNode: SqlNode? = getAggregate(select)
        if (aggregateNode != null) {
            when (modality) {
                STREAM -> {
                    val groupList: SqlNodeList = select.getGroup()
                    if (groupList == null
                        || !SqlValidatorUtil.containsMonotonic(scope, groupList)
                    ) {
                        return if (fail) {
                            throw newValidationError(
                                aggregateNode,
                                Static.RESOURCE.streamMustGroupByMonotonic()
                            )
                        } else {
                            false
                        }
                    }
                }
                else -> {}
            }
        }

        // Make sure that ORDER BY is possible.
        val orderList: SqlNodeList = select.getOrderList()
        if (orderList != null && orderList.size() > 0) {
            when (modality) {
                STREAM -> if (!hasSortedPrefix(scope, orderList)) {
                    return if (fail) {
                        throw newValidationError(
                            orderList.get(0),
                            Static.RESOURCE.streamMustOrderByMonotonic()
                        )
                    } else {
                        false
                    }
                }
                else -> {}
            }
        }
        return true
    }

    @SuppressWarnings(["unchecked", "rawtypes"])
    protected fun validateWindowClause(select: SqlSelect) {
        val windowList: SqlNodeList = select.getWindowList()
        if (windowList.isEmpty()) {
            return
        }
        val windowScope: SelectScope = requireNonNull(
            getFromScope(select)
        ) { "fromScope for $select" } as SelectScope

        // 1. ensure window names are simple
        // 2. ensure they are unique within this scope
        for (window in windowList) {
            val declName: SqlIdentifier = requireNonNull(
                window.getDeclName()
            ) { "window.getDeclName() for $window" }
            if (!declName.isSimple()) {
                throw newValidationError(declName, RESOURCE.windowNameMustBeSimple())
            }
            if (windowScope.existingWindowName(declName.toString())) {
                throw newValidationError(declName, RESOURCE.duplicateWindowName())
            } else {
                windowScope.addWindowName(declName.toString())
            }
        }

        // 7.10 rule 2
        // Check for pairs of windows which are equivalent.
        for (i in 0 until windowList.size()) {
            val window1: SqlNode = windowList.get(i)
            for (j in i + 1 until windowList.size()) {
                val window2: SqlNode = windowList.get(j)
                if (window1.equalsDeep(window2, Litmus.IGNORE)) {
                    throw newValidationError(window2, RESOURCE.dupWindowSpec())
                }
            }
        }
        for (window in windowList) {
            val expandedOrderList: SqlNodeList = expand(window.getOrderList(), windowScope) as SqlNodeList
            window.setOrderList(expandedOrderList)
            expandedOrderList.validate(this, windowScope)
            val expandedPartitionList: SqlNodeList = expand(window.getPartitionList(), windowScope) as SqlNodeList
            window.setPartitionList(expandedPartitionList)
            expandedPartitionList.validate(this, windowScope)
        }

        // Hand off to validate window spec components
        windowList.validate(this, windowScope)
    }

    @Override
    override fun validateWith(with: SqlWith, scope: SqlValidatorScope?) {
        val namespace: SqlValidatorNamespace = getNamespaceOrThrow(with)
        validateNamespace(namespace, unknownType)
    }

    @Override
    override fun validateWithItem(withItem: SqlWithItem) {
        val columnList: SqlNodeList = withItem.columnList
        if (columnList != null) {
            val rowType: RelDataType = getValidatedNodeType(withItem.query)
            val fieldCount: Int = rowType.getFieldCount()
            if (columnList.size() !== fieldCount) {
                throw newValidationError(
                    columnList,
                    RESOURCE.columnCountMismatch()
                )
            }
            SqlValidatorUtil.checkIdentifierListForDuplicates(
                columnList, validationErrorFunction
            )
        } else {
            // Luckily, field names have not been make unique yet.
            val fieldNames: List<String> = getValidatedNodeType(withItem.query).getFieldNames()
            val i: Int = Util.firstDuplicate(fieldNames)
            if (i >= 0) {
                throw newValidationError(
                    withItem.query,
                    RESOURCE.duplicateColumnAndNoColumnList(fieldNames[i])
                )
            }
        }
    }

    @Override
    fun validateSequenceValue(scope: SqlValidatorScope, id: SqlIdentifier) {
        // Resolve identifier as a table.
        val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
        scope.resolveTable(
            id.names, catalogReader.nameMatcher(),
            SqlValidatorScope.Path.EMPTY, resolved
        )
        if (resolved.count() !== 1) {
            throw newValidationError(id, RESOURCE.tableNameNotFound(id.toString()))
        }
        // We've found a table. But is it a sequence?
        val ns: SqlValidatorNamespace = resolved.only().namespace
        if (ns is TableNamespace) {
            val table: Table = getTable(ns).table()
            when (table.getJdbcTableType()) {
                SEQUENCE, TEMPORARY_SEQUENCE -> return
                else -> {}
            }
        }
        throw newValidationError(id, RESOURCE.notASequence(id.toString()))
    }

    @Override
    @Nullable
    override fun getWithScope(withItem: SqlNode): SqlValidatorScope {
        assert(withItem.getKind() === SqlKind.WITH_ITEM)
        return scopes.get(withItem)
    }

    @Override
    fun getTypeCoercion(): TypeCoercion {
        assert(config.typeCoercionEnabled())
        return typeCoercion
    }

    @Override
    override fun config(): Config {
        return config
    }

    @Override
    override fun transform(transform: UnaryOperator<Config?>): SqlValidator {
        config = transform.apply(config)
        return this
    }

    /**
     * Validates the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     */
    protected fun validateOrderList(select: SqlSelect) {
        // ORDER BY is validated in a scope where aliases in the SELECT clause
        // are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
        // is valid.
        val orderList: SqlNodeList = select.getOrderList() ?: return
        if (!shouldAllowIntermediateOrderBy()) {
            if (!cursorSet.contains(select)) {
                throw newValidationError(select, RESOURCE.invalidOrderByPos())
            }
        }
        val orderScope: SqlValidatorScope = getOrderScope(select)
        requireNonNull(orderScope, "orderScope")
        val expandList: List<SqlNode> = ArrayList()
        for (orderItem in orderList) {
            val expandedOrderItem: SqlNode = expand(orderItem, orderScope)
            expandList.add(expandedOrderItem)
        }
        val expandedOrderList = SqlNodeList(
            expandList,
            orderList.getParserPosition()
        )
        select.setOrderBy(expandedOrderList)
        for (orderItem in expandedOrderList) {
            validateOrderItem(select, orderItem)
        }
    }

    /**
     * Validates an item in the GROUP BY clause of a SELECT statement.
     *
     * @param select Select statement
     * @param groupByItem GROUP BY clause item
     */
    private fun validateGroupByItem(select: SqlSelect, groupByItem: SqlNode) {
        val groupByScope: SqlValidatorScope = getGroupScope(select)
        validateGroupByExpr(groupByItem, groupByScope)
        groupByScope.validateExpr(groupByItem)
    }

    private fun validateGroupByExpr(
        groupByItem: SqlNode,
        groupByScope: SqlValidatorScope
    ) {
        when (groupByItem.getKind()) {
            GROUPING_SETS, ROLLUP, CUBE -> {
                val call: SqlCall = groupByItem as SqlCall
                for (operand in call.getOperandList()) {
                    validateExpr(operand, groupByScope)
                }
            }
            else -> validateExpr(groupByItem, groupByScope)
        }
    }

    /**
     * Validates an item in the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     * @param orderItem ORDER BY clause item
     */
    private fun validateOrderItem(select: SqlSelect, orderItem: SqlNode) {
        when (orderItem.getKind()) {
            DESCENDING -> {
                validateFeature(
                    RESOURCE.sQLConformance_OrderByDesc(),
                    orderItem.getParserPosition()
                )
                validateOrderItem(
                    select,
                    (orderItem as SqlCall).operand(0)
                )
                return
            }
            else -> {}
        }
        val orderScope: SqlValidatorScope = getOrderScope(select)
        validateExpr(orderItem, orderScope)
    }

    @Override
    override fun expandOrderExpr(select: SqlSelect, orderExpr: SqlNode): SqlNode {
        val newSqlNode: SqlNode = OrderExpressionExpander(select, orderExpr).go()
        if (newSqlNode !== orderExpr) {
            val scope: SqlValidatorScope = getOrderScope(select)
            inferUnknownTypes(unknownType, scope, newSqlNode)
            val type: RelDataType? = deriveType(scope, newSqlNode)
            setValidatedNodeType(newSqlNode, type)
        }
        return newSqlNode
    }

    /**
     * Validates the GROUP BY clause of a SELECT statement. This method is
     * called even if no GROUP BY clause is present.
     */
    protected fun validateGroupClause(select: SqlSelect) {
        var groupList: SqlNodeList? = select.getGroup() ?: return
        val clause = "GROUP BY"
        validateNoAggs(aggOrOverFinder, groupList, clause)
        val groupScope: SqlValidatorScope = getGroupScope(select)

        // expand the expression in group list.
        val expandedList: List<SqlNode> = ArrayList()
        for (groupItem in groupList) {
            val expandedItem: SqlNode = expandGroupByOrHavingExpr(groupItem, groupScope, select, false)
            expandedList.add(expandedItem)
        }
        groupList = SqlNodeList(expandedList, groupList.getParserPosition())
        select.setGroupBy(groupList)
        inferUnknownTypes(unknownType, groupScope, groupList)
        for (groupItem in expandedList) {
            validateGroupByItem(select, groupItem)
        }

        // Nodes in the GROUP BY clause are expressions except if they are calls
        // to the GROUPING SETS, ROLLUP or CUBE operators; this operators are not
        // expressions, because they do not have a type.
        for (node in groupList) {
            when (node.getKind()) {
                GROUPING_SETS, ROLLUP, CUBE -> node.validate(this, groupScope)
                else -> node.validateExpr(this, groupScope)
            }
        }

        // Derive the type of each GROUP BY item. We don't need the type, but
        // it resolves functions, and that is necessary for deducing
        // monotonicity.
        val selectScope: SqlValidatorScope = getSelectScope(select)
        var aggregatingScope: AggregatingSelectScope? = null
        if (selectScope is AggregatingSelectScope) {
            aggregatingScope = selectScope as AggregatingSelectScope
        }
        for (groupItem in groupList) {
            if (groupItem is SqlNodeList
                && (groupItem as SqlNodeList).size() === 0
            ) {
                continue
            }
            validateGroupItem(groupScope, aggregatingScope, groupItem)
        }
        val agg: SqlNode = aggFinder.findAgg(groupList)
        if (agg != null) {
            throw newValidationError(agg, RESOURCE.aggregateIllegalInClause(clause))
        }
    }

    private fun validateGroupItem(
        groupScope: SqlValidatorScope,
        @Nullable aggregatingScope: AggregatingSelectScope?,
        groupItem: SqlNode
    ) {
        when (groupItem.getKind()) {
            GROUPING_SETS, ROLLUP, CUBE -> validateGroupingSets(groupScope, aggregatingScope, groupItem as SqlCall)
            else -> {
                if (groupItem is SqlNodeList) {
                    break
                }
                val type: RelDataType? = deriveType(groupScope, groupItem)
                setValidatedNodeType(groupItem, type)
            }
        }
    }

    private fun validateGroupingSets(
        groupScope: SqlValidatorScope,
        @Nullable aggregatingScope: AggregatingSelectScope?, groupItem: SqlCall
    ) {
        for (node in groupItem.getOperandList()) {
            validateGroupItem(groupScope, aggregatingScope, node)
        }
    }

    protected fun validateWhereClause(select: SqlSelect) {
        // validate WHERE clause
        val where: SqlNode = select.getWhere() ?: return
        val whereScope: SqlValidatorScope = getWhereScope(select)
        val expandedWhere: SqlNode = expand(where, whereScope)
        select.setWhere(expandedWhere)
        validateWhereOrOn(whereScope, expandedWhere, "WHERE")
    }

    protected fun validateWhereOrOn(
        scope: SqlValidatorScope,
        condition: SqlNode?,
        clause: String
    ) {
        validateNoAggs(aggOrOverOrGroupFinder, condition, clause)
        inferUnknownTypes(
            booleanType,
            scope,
            condition
        )
        condition.validate(this, scope)
        val type: RelDataType? = deriveType(scope, condition)
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(condition, RESOURCE.condMustBeBoolean(clause))
        }
    }

    protected fun validateHavingClause(select: SqlSelect) {
        // HAVING is validated in the scope after groups have been created.
        // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
        // deptno HAVING empno = 10", the reference to 'empno' in the HAVING
        // clause is illegal.
        var having: SqlNode? = select.getHaving() ?: return
        val havingScope: AggregatingScope = getSelectScope(select) as AggregatingScope
        if (config.sqlConformance().isHavingAlias()) {
            val newExpr: SqlNode = expandGroupByOrHavingExpr(having, havingScope, select, true)
            if (having !== newExpr) {
                having = newExpr
                select.setHaving(newExpr)
            }
        }
        havingScope.checkAggregateExpr(having, true)
        inferUnknownTypes(
            booleanType,
            havingScope,
            having
        )
        having.validate(this, havingScope)
        val type: RelDataType? = deriveType(havingScope, having)
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(having, RESOURCE.havingMustBeBoolean())
        }
    }

    protected fun validateSelectList(
        selectItems: SqlNodeList,
        select: SqlSelect,
        targetRowType: RelDataType?
    ): RelDataType {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        val selectScope: SqlValidatorScope = getSelectScope(select)
        val expandedSelectItems: List<SqlNode> = ArrayList()
        val aliases: Set<String> = HashSet()
        val fieldList: List<Map.Entry<String, RelDataType>> = ArrayList()
        for (selectItem in selectItems) {
            if (selectItem is SqlSelect) {
                handleScalarSubQuery(
                    select,
                    selectItem as SqlSelect,
                    expandedSelectItems,
                    aliases,
                    fieldList
                )
            } else {
                // Use the field list size to record the field index
                // because the select item may be a STAR(*), which could have been expanded.
                val fieldIdx: Int = fieldList.size()
                val fieldType: RelDataType = if (targetRowType.isStruct()
                    && targetRowType.getFieldCount() > fieldIdx
                ) targetRowType.getFieldList().get(fieldIdx).getType() else unknownType
                expandSelectItem(
                    selectItem,
                    select,
                    fieldType,
                    expandedSelectItems,
                    aliases,
                    fieldList,
                    false
                )
            }
        }

        // Create the new select list with expanded items.  Pass through
        // the original parser position so that any overall failures can
        // still reference the original input text.
        val newSelectList = SqlNodeList(
            expandedSelectItems,
            selectItems.getParserPosition()
        )
        if (config.identifierExpansion()) {
            select.setSelectList(newSelectList)
        }
        getRawSelectScopeNonNull(select).setExpandedSelectList(expandedSelectItems)

        // TODO: when SELECT appears as a value sub-query, should be using
        // something other than unknownType for targetRowType
        inferUnknownTypes(targetRowType, selectScope, newSelectList)
        for (selectItem in expandedSelectItems) {
            validateNoAggs(groupFinder, selectItem, "SELECT")
            validateExpr(selectItem, selectScope)
        }
        return typeFactory.createStructType(fieldList)
    }

    /**
     * Validates an expression.
     *
     * @param expr  Expression
     * @param scope Scope in which expression occurs
     */
    private fun validateExpr(expr: SqlNode, scope: SqlValidatorScope) {
        if (expr is SqlCall) {
            val op: SqlOperator = (expr as SqlCall).getOperator()
            if (op.isAggregator() && op.requiresOver()) {
                throw newValidationError(
                    expr,
                    RESOURCE.absentOverClause()
                )
            }
            if (op is SqlTableFunction) {
                throw RESOURCE.cannotCallTableFunctionHere(op.getName()).ex()
            }
        }

        // Call on the expression to validate itself.
        expr.validateExpr(this, scope)

        // Perform any validation specific to the scope. For example, an
        // aggregating scope requires that expressions are valid aggregations.
        scope.validateExpr(expr)
    }

    /**
     * Processes SubQuery found in Select list. Checks that is actually Scalar
     * sub-query and makes proper entries in each of the 3 lists used to create
     * the final rowType entry.
     *
     * @param parentSelect        base SqlSelect item
     * @param selectItem          child SqlSelect from select list
     * @param expandedSelectItems Select items after processing
     * @param aliasList           built from user or system values
     * @param fieldList           Built up entries for each select list entry
     */
    private fun handleScalarSubQuery(
        parentSelect: SqlSelect,
        selectItem: SqlSelect,
        expandedSelectItems: List<SqlNode>,
        aliasList: Set<String>,
        fieldList: List<Map.Entry<String, RelDataType>>
    ) {
        // A scalar sub-query only has one output column.
        if (1 != SqlNonNullableAccessors.getSelectList(selectItem).size()) {
            throw newValidationError(
                selectItem,
                RESOURCE.onlyScalarSubQueryAllowed()
            )
        }

        // No expansion in this routine just append to list.
        expandedSelectItems.add(selectItem)

        // Get or generate alias and add to list.
        val alias = deriveAliasNonNull(
            selectItem,
            aliasList.size()
        )
        aliasList.add(alias)
        val scope: SelectScope = getWhereScope(parentSelect) as SelectScope
        val type: RelDataType? = deriveType(scope, selectItem)
        setValidatedNodeType(selectItem, type)
        assert(type is RelRecordType)
        val rec: RelRecordType = type as RelRecordType
        var nodeType: RelDataType = rec.getFieldList().get(0).getType()
        nodeType = typeFactory.createTypeWithNullability(nodeType, true)
        fieldList.add(Pair.of(alias, nodeType))
    }

    /**
     * Derives a row-type for INSERT and UPDATE operations.
     *
     * @param table            Target table for INSERT/UPDATE
     * @param targetColumnList List of target columns, or null if not specified
     * @param append           Whether to append fields to those in `
     * baseRowType`
     * @return Rowtype
     */
    protected fun createTargetRowType(
        table: SqlValidatorTable,
        @Nullable targetColumnList: SqlNodeList?,
        append: Boolean
    ): RelDataType {
        val baseRowType: RelDataType = table.getRowType()
        if (targetColumnList == null) {
            return baseRowType
        }
        val targetFields: List<RelDataTypeField> = baseRowType.getFieldList()
        val fields: List<Map.Entry<String, RelDataType>> = ArrayList()
        if (append) {
            for (targetField in targetFields) {
                fields.add(
                    Pair.of(
                        SqlUtil.deriveAliasFromOrdinal(fields.size()),
                        targetField.getType()
                    )
                )
            }
        }
        val assignedFields: Set<Integer> = HashSet()
        val relOptTable: RelOptTable? = if (table is RelOptTable) table as RelOptTable else null
        for (node in targetColumnList) {
            val id: SqlIdentifier = node as SqlIdentifier
            val targetField: RelDataTypeField = SqlValidatorUtil.getTargetField(
                baseRowType, typeFactory, id, catalogReader, relOptTable
            )
                ?: throw newValidationError(
                    id,
                    RESOURCE.unknownTargetColumn(id.toString())
                )
            if (!assignedFields.add(targetField.getIndex())) {
                throw newValidationError(
                    id,
                    RESOURCE.duplicateTargetColumn(targetField.getName())
                )
            }
            fields.add(targetField)
        }
        return typeFactory.createStructType(fields)
    }

    @Override
    override fun validateInsert(insert: SqlInsert) {
        val targetNamespace: SqlValidatorNamespace = getNamespaceOrThrow(insert)
        validateNamespace(targetNamespace, unknownType)
        val relOptTable: RelOptTable = SqlValidatorUtil.getRelOptTable(
            targetNamespace, catalogReader.unwrap(CatalogReader::class.java), null, null
        )
        val table: SqlValidatorTable =
            if (relOptTable == null) getTable(targetNamespace) else relOptTable.unwrapOrThrow(
                SqlValidatorTable::class.java
            )

        // INSERT has an optional column name list.  If present then
        // reduce the rowtype to the columns specified.  If not present
        // then the entire target rowtype is used.
        val targetRowType: RelDataType = createTargetRowType(
            table,
            insert.getTargetColumnList(),
            false
        )
        val source: SqlNode = insert.getSource()
        if (source is SqlSelect) {
            val sqlSelect: SqlSelect = source as SqlSelect
            validateSelect(sqlSelect, targetRowType)
        } else {
            val scope: SqlValidatorScope = scopes.get(source)
            validateQuery(source, scope, targetRowType)
        }

        // REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
        // discarding the type inferred by inferUnknownTypes (which was invoked
        // from validateSelect above).  It would be better if that information
        // were used here so that we never saw any untyped nulls during
        // checkTypeAssignment.
        val sourceRowType: RelDataType = getNamespaceOrThrow(source).getRowType()
        val logicalTargetRowType: RelDataType = getLogicalTargetRowType(targetRowType, insert)
        setValidatedNodeType(insert, logicalTargetRowType)
        val logicalSourceRowType: RelDataType = getLogicalSourceRowType(sourceRowType, insert)
        val strategies: List<ColumnStrategy> = table.unwrapOrThrow(RelOptTable::class.java).getColumnStrategies()
        val realTargetRowType: RelDataType = typeFactory.createStructType(
            logicalTargetRowType.getFieldList()
                .stream().filter { f -> strategies[f.getIndex()].canInsertInto() }
                .collect(Collectors.toList()))
        val targetRowTypeToValidate: RelDataType =
            if (logicalSourceRowType.getFieldCount() === logicalTargetRowType.getFieldCount()) logicalTargetRowType else realTargetRowType
        checkFieldCount(
            insert.getTargetTable(), table, strategies,
            targetRowTypeToValidate, realTargetRowType,
            source, logicalSourceRowType, logicalTargetRowType
        )
        checkTypeAssignment(
            scopes.get(source),
            table,
            logicalSourceRowType,
            targetRowTypeToValidate,
            insert
        )
        checkConstraint(table, source, logicalTargetRowType)
        validateAccess(insert.getTargetTable(), table, SqlAccessEnum.INSERT)

        // Refresh the insert row type to keep sync with source.
        setValidatedNodeType(insert, targetRowTypeToValidate)
    }

    /**
     * Validates insert values against the constraint of a modifiable view.
     *
     * @param validatorTable Table that may wrap a ModifiableViewTable
     * @param source        The values being inserted
     * @param targetRowType The target type for the view
     */
    private fun checkConstraint(
        validatorTable: SqlValidatorTable,
        source: SqlNode,
        targetRowType: RelDataType
    ) {
        val modifiableViewTable: ModifiableViewTable = validatorTable.unwrap(ModifiableViewTable::class.java)
        if (modifiableViewTable != null && source is SqlCall) {
            val table: Table = modifiableViewTable.getTable()
            val tableRowType: RelDataType = table.getRowType(typeFactory)
            val tableFields: List<RelDataTypeField> = tableRowType.getFieldList()

            // Get the mapping from column indexes of the underlying table
            // to the target columns and view constraints.
            val tableIndexToTargetField: Map<Integer, RelDataTypeField> =
                SqlValidatorUtil.getIndexToFieldMap(tableFields, targetRowType)
            val projectMap: Map<Integer, RexNode> =
                RelOptUtil.getColumnConstraints(modifiableViewTable, targetRowType, typeFactory)

            // Determine columns (indexed to the underlying table) that need
            // to be validated against the view constraint.
            @SuppressWarnings("RedundantCast") val targetColumns: ImmutableBitSet =
                ImmutableBitSet.of(tableIndexToTargetField.keySet() as Iterable<Integer?>?)
            @SuppressWarnings("RedundantCast") val constrainedColumns: ImmutableBitSet =
                ImmutableBitSet.of(projectMap.keySet() as Iterable<Integer?>?)
            @SuppressWarnings("assignment.type.incompatible") val constrainedTargetColumns: List<Integer> =
                targetColumns.intersect(constrainedColumns).asList()

            // Validate insert values against the view constraint.
            val values: List<SqlNode> = (source as SqlCall).getOperandList()
            for (colIndex in constrainedTargetColumns) {
                val colName: String = tableFields[colIndex].getName()
                val targetField: RelDataTypeField? = tableIndexToTargetField[colIndex]
                for (row in values) {
                    val call: SqlCall = row as SqlCall
                    val sourceValue: SqlNode = call.operand(targetField.getIndex())
                    val validationError: ValidationError = ValidationError(
                        sourceValue,
                        RESOURCE.viewConstraintNotSatisfied(
                            colName,
                            Util.last(validatorTable.getQualifiedName())
                        )
                    )
                    RelOptUtil.validateValueAgainstConstraint(
                        sourceValue,
                        projectMap[colIndex], validationError
                    )
                }
            }
        }
    }

    /**
     * Validates updates against the constraint of a modifiable view.
     *
     * @param validatorTable A [SqlValidatorTable] that may wrap a
     * ModifiableViewTable
     * @param update         The UPDATE parse tree node
     * @param targetRowType  The target type
     */
    private fun checkConstraint(
        validatorTable: SqlValidatorTable,
        update: SqlUpdate,
        targetRowType: RelDataType
    ) {
        val modifiableViewTable: ModifiableViewTable = validatorTable.unwrap(ModifiableViewTable::class.java)
        if (modifiableViewTable != null) {
            val table: Table = modifiableViewTable.getTable()
            val tableRowType: RelDataType = table.getRowType(typeFactory)
            val projectMap: Map<Integer, RexNode> = RelOptUtil.getColumnConstraints(
                modifiableViewTable, targetRowType,
                typeFactory
            )
            val nameToIndex: Map<String, Integer> = SqlValidatorUtil.mapNameToIndex(tableRowType.getFieldList())

            // Validate update values against the view constraint.
            val targetNames: List<String> = SqlIdentifier.simpleNames(update.getTargetColumnList())
            val sources: List<SqlNode> = update.getSourceExpressionList()
            Pair.forEach(targetNames, sources) { columnName, expr ->
                val columnIndex: Integer? = nameToIndex[columnName]
                if (projectMap.containsKey(columnIndex)) {
                    val columnConstraint: RexNode? = projectMap[columnIndex]
                    val validationError: ValidationError = ValidationError(
                        expr,
                        RESOURCE.viewConstraintNotSatisfied(
                            columnName,
                            Util.last(validatorTable.getQualifiedName())
                        )
                    )
                    RelOptUtil.validateValueAgainstConstraint(
                        expr,
                        columnConstraint, validationError
                    )
                }
            }
        }
    }

    /**
     * Check the field count of sql insert source and target node row type.
     *
     * @param node                    target table sql identifier
     * @param table                   target table
     * @param strategies              column strategies of target table
     * @param targetRowTypeToValidate row type to validate mainly for column strategies
     * @param realTargetRowType       target table row type exclusive virtual columns
     * @param source                  source node
     * @param logicalSourceRowType    source node row type
     * @param logicalTargetRowType    logical target row type, contains only target columns if
     * they are specified or if the sql dialect allows subset insert,
     * make a subset of fields(start from the left first field) whose
     * length is equals with the source row type fields number
     */
    private fun checkFieldCount(
        node: SqlNode, table: SqlValidatorTable,
        strategies: List<ColumnStrategy>, targetRowTypeToValidate: RelDataType,
        realTargetRowType: RelDataType, source: SqlNode,
        logicalSourceRowType: RelDataType, logicalTargetRowType: RelDataType
    ) {
        val sourceFieldCount: Int = logicalSourceRowType.getFieldCount()
        val targetFieldCount: Int = logicalTargetRowType.getFieldCount()
        val targetRealFieldCount: Int = realTargetRowType.getFieldCount()
        if (sourceFieldCount != targetFieldCount
            && sourceFieldCount != targetRealFieldCount
        ) {
            // Allows the source row fields count to be equal with either
            // the logical or the real(excludes columns that can not insert into)
            // target row fields count.
            throw newValidationError(
                node,
                RESOURCE.unmatchInsertColumn(targetFieldCount, sourceFieldCount)
            )
        }
        // Ensure that non-nullable fields are targeted.
        for (field in table.getRowType().getFieldList()) {
            val targetField: RelDataTypeField = targetRowTypeToValidate.getField(field.getName(), true, false)
            when (strategies[field.getIndex()]) {
                NOT_NULLABLE -> {
                    assert(!field.getType().isNullable())
                    if (targetField == null) {
                        throw newValidationError(
                            node,
                            RESOURCE.columnNotNullable(field.getName())
                        )
                    }
                }
                NULLABLE -> assert(field.getType().isNullable())
                VIRTUAL, STORED -> if (targetField != null
                    && !isValuesWithDefault(source, targetField.getIndex())
                ) {
                    throw newValidationError(
                        node,
                        RESOURCE.insertIntoAlwaysGenerated(field.getName())
                    )
                }
                else -> {}
            }
        }
    }

    protected fun getLogicalTargetRowType(
        targetRowType: RelDataType,
        insert: SqlInsert
    ): RelDataType {
        return if (insert.getTargetColumnList() == null
            && config.sqlConformance().isInsertSubsetColumnsAllowed()
        ) {
            // Target an implicit subset of columns.
            val source: SqlNode = insert.getSource()
            val sourceRowType: RelDataType = getNamespaceOrThrow(source).getRowType()
            val logicalSourceRowType: RelDataType = getLogicalSourceRowType(sourceRowType, insert)
            val implicitTargetRowType: RelDataType = typeFactory.createStructType(
                targetRowType.getFieldList()
                    .subList(0, logicalSourceRowType.getFieldCount())
            )
            val targetNamespace: SqlValidatorNamespace = getNamespaceOrThrow(insert)
            validateNamespace(targetNamespace, implicitTargetRowType)
            implicitTargetRowType
        } else {
            // Either the set of columns are explicitly targeted, or target the full
            // set of columns.
            targetRowType
        }
    }

    protected fun getLogicalSourceRowType(
        sourceRowType: RelDataType,
        insert: SqlInsert?
    ): RelDataType {
        return sourceRowType
    }

    /**
     * Checks the type assignment of an INSERT or UPDATE query.
     *
     *
     * Skip the virtual columns(can not insert into) type assignment
     * check if the source fields count equals with
     * the real target table fields count, see how #checkFieldCount was used.
     *
     * @param sourceScope   Scope of query source which is used to infer node type
     * @param table         Target table
     * @param sourceRowType Source row type
     * @param targetRowType Target row type, it should either contain all the virtual columns
     * (can not insert into) or exclude all the virtual columns
     * @param query The query
     */
    protected fun checkTypeAssignment(
        @Nullable sourceScope: SqlValidatorScope?,
        table: SqlValidatorTable,
        sourceRowType: RelDataType,
        targetRowType: RelDataType,
        query: SqlNode
    ) {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
        // representing system-maintained columns, so stop after all sources
        // matched
        var sourceRowType: RelDataType = sourceRowType
        var targetRowType: RelDataType = targetRowType
        var isUpdateModifiableViewTable = false
        if (query is SqlUpdate) {
            val targetColumnList: SqlNodeList = (query as SqlUpdate).getTargetColumnList()
            if (targetColumnList != null) {
                val targetColumnCnt: Int = targetColumnList.size()
                targetRowType = SqlTypeUtil.extractLastNFields(
                    typeFactory, targetRowType,
                    targetColumnCnt
                )
                sourceRowType = SqlTypeUtil.extractLastNFields(
                    typeFactory, sourceRowType,
                    targetColumnCnt
                )
            }
            isUpdateModifiableViewTable = table.unwrap(ModifiableViewTable::class.java) != null
        }
        if (SqlTypeUtil.equalAsStructSansNullability(
                typeFactory,
                sourceRowType,
                targetRowType,
                null
            )
        ) {
            // Returns early if source and target row type equals sans nullability.
            return
        }
        if (config.typeCoercionEnabled() && !isUpdateModifiableViewTable) {
            // Try type coercion first if implicit type coercion is allowed.
            val coerced: Boolean = typeCoercion.querySourceCoercion(
                sourceScope,
                sourceRowType,
                targetRowType,
                query
            )
            if (coerced) {
                return
            }
        }

        // Fall back to default behavior: compare the type families.
        val sourceFields: List<RelDataTypeField> = sourceRowType.getFieldList()
        val targetFields: List<RelDataTypeField> = targetRowType.getFieldList()
        val sourceCount: Int = sourceFields.size()
        for (i in 0 until sourceCount) {
            val sourceType: RelDataType = sourceFields[i].getType()
            val targetType: RelDataType = targetFields[i].getType()
            if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
                val node: SqlNode = getNthExpr(query, i, sourceCount)
                if (node is SqlDynamicParam) {
                    continue
                }
                val targetTypeString: String
                val sourceTypeString: String
                if (SqlTypeUtil.areCharacterSetsMismatched(
                        sourceType,
                        targetType
                    )
                ) {
                    sourceTypeString = sourceType.getFullTypeString()
                    targetTypeString = targetType.getFullTypeString()
                } else {
                    sourceTypeString = sourceType.toString()
                    targetTypeString = targetType.toString()
                }
                throw newValidationError(
                    node,
                    RESOURCE.typeNotAssignable(
                        targetFields[i].getName(), targetTypeString,
                        sourceFields[i].getName(), sourceTypeString
                    )
                )
            }
        }
    }

    @Override
    override fun validateDelete(call: SqlDelete) {
        val sqlSelect: SqlSelect = SqlNonNullableAccessors.getSourceSelect(call)
        validateSelect(sqlSelect, unknownType)
        val targetNamespace: SqlValidatorNamespace = getNamespaceOrThrow(call)
        validateNamespace(targetNamespace, unknownType)
        val table: SqlValidatorTable = targetNamespace.getTable()
        validateAccess(call.getTargetTable(), table, SqlAccessEnum.DELETE)
    }

    @Override
    override fun validateUpdate(call: SqlUpdate) {
        val targetNamespace: SqlValidatorNamespace = getNamespaceOrThrow(call)
        validateNamespace(targetNamespace, unknownType)
        val relOptTable: RelOptTable = SqlValidatorUtil.getRelOptTable(
            targetNamespace, castNonNull(catalogReader.unwrap(CatalogReader::class.java)),
            null, null
        )
        val table: SqlValidatorTable =
            if (relOptTable == null) getTable(targetNamespace) else relOptTable.unwrapOrThrow(
                SqlValidatorTable::class.java
            )
        val targetRowType: RelDataType = createTargetRowType(
            table,
            call.getTargetColumnList(),
            true
        )
        val select: SqlSelect = SqlNonNullableAccessors.getSourceSelect(call)
        validateSelect(select, targetRowType)
        val sourceRowType: RelDataType = getValidatedNodeType(select)
        checkTypeAssignment(
            scopes.get(select),
            table,
            sourceRowType,
            targetRowType,
            call
        )
        checkConstraint(table, call, targetRowType)
        validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE)
    }

    @Override
    override fun validateMerge(call: SqlMerge) {
        val sqlSelect: SqlSelect = SqlNonNullableAccessors.getSourceSelect(call)
        // REVIEW zfong 5/25/06 - Does an actual type have to be passed into
        // validateSelect()?

        // REVIEW jvs 6-June-2006:  In general, passing unknownType like
        // this means we won't be able to correctly infer the types
        // for dynamic parameter markers (SET x = ?).  But
        // maybe validateUpdate and validateInsert below will do
        // the job?

        // REVIEW ksecretan 15-July-2011: They didn't get a chance to
        // since validateSelect() would bail.
        // Let's use the update/insert targetRowType when available.
        val targetNamespace: IdentifierNamespace = getNamespaceOrThrow(call.getTargetTable()) as IdentifierNamespace
        validateNamespace(targetNamespace, unknownType)
        val table: SqlValidatorTable = targetNamespace.getTable()
        validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE)
        var targetRowType: RelDataType = unknownType
        val updateCall: SqlUpdate = call.getUpdateCall()
        if (updateCall != null) {
            requireNonNull(table) { "ns.getTable() for $targetNamespace" }
            targetRowType = createTargetRowType(
                table,
                updateCall.getTargetColumnList(),
                true
            )
        }
        val insertCall: SqlInsert = call.getInsertCall()
        if (insertCall != null) {
            requireNonNull(table) { "ns.getTable() for $targetNamespace" }
            targetRowType = createTargetRowType(
                table,
                insertCall.getTargetColumnList(),
                false
            )
        }
        validateSelect(sqlSelect, targetRowType)
        val updateCallAfterValidate: SqlUpdate = call.getUpdateCall()
        if (updateCallAfterValidate != null) {
            validateUpdate(updateCallAfterValidate)
        }
        val insertCallAfterValidate: SqlInsert = call.getInsertCall()
        if (insertCallAfterValidate != null) {
            validateInsert(insertCallAfterValidate)
        }
    }

    /**
     * Validates access to a table.
     *
     * @param table          Table
     * @param requiredAccess Access requested on table
     */
    private fun validateAccess(
        node: SqlNode,
        @Nullable table: SqlValidatorTable?,
        requiredAccess: SqlAccessEnum
    ) {
        if (table != null) {
            val access: SqlAccessType = table.getAllowedAccess()
            if (!access.allowsAccess(requiredAccess)) {
                throw newValidationError(
                    node,
                    RESOURCE.accessNotAllowed(
                        requiredAccess.name(),
                        table.getQualifiedName().toString()
                    )
                )
            }
        }
    }

    /**
     * Validates snapshot to a table.
     *
     * @param node  The node to validate
     * @param scope Validator scope to derive type
     * @param ns    The namespace to lookup table
     */
    private fun validateSnapshot(
        node: SqlNode,
        @Nullable scope: SqlValidatorScope,
        ns: SqlValidatorNamespace
    ) {
        if (node.getKind() === SqlKind.SNAPSHOT) {
            val snapshot: SqlSnapshot = node as SqlSnapshot
            val period: SqlNode = snapshot.getPeriod()
            val dataType: RelDataType? = deriveType(requireNonNull(scope, "scope"), period)
            if (dataType.getSqlTypeName() !== SqlTypeName.TIMESTAMP) {
                throw newValidationError(
                    period,
                    Static.RESOURCE.illegalExpressionForTemporal(dataType.getSqlTypeName().getName())
                )
            }
            val table: SqlValidatorTable = getTable(ns)
            if (!table.isTemporal()) {
                val qualifiedName: List<String> = table.getQualifiedName()
                val tableName = qualifiedName[qualifiedName.size() - 1]
                throw newValidationError(
                    snapshot.getTableRef(),
                    Static.RESOURCE.notTemporalTable(tableName)
                )
            }
        }
    }

    /**
     * Validates a VALUES clause.
     *
     * @param node          Values clause
     * @param targetRowType Row type which expression must conform to
     * @param scope         Scope within which clause occurs
     */
    protected fun validateValues(
        node: SqlCall,
        targetRowType: RelDataType,
        scope: SqlValidatorScope
    ) {
        var targetRowType: RelDataType = targetRowType
        assert(node.getKind() === SqlKind.VALUES)
        val operands: List<SqlNode> = node.getOperandList()
        for (operand in operands) {
            if (!(operand.getKind() === SqlKind.ROW)) {
                throw Util.needToImplement(
                    "Values function where operands are scalars"
                )
            }
            val rowConstructor: SqlCall = operand as SqlCall
            if (config.sqlConformance().isInsertSubsetColumnsAllowed()
                && targetRowType.isStruct()
                && rowConstructor.operandCount() < targetRowType.getFieldCount()
            ) {
                targetRowType = typeFactory.createStructType(
                    targetRowType.getFieldList()
                        .subList(0, rowConstructor.operandCount())
                )
            } else if (targetRowType.isStruct()
                && rowConstructor.operandCount() !== targetRowType.getFieldCount()
            ) {
                return
            }
            inferUnknownTypes(
                targetRowType,
                scope,
                rowConstructor
            )
            if (targetRowType.isStruct()) {
                for (pair in Pair.zip(
                    rowConstructor.getOperandList(),
                    targetRowType.getFieldList()
                )) {
                    if (!pair.right.getType().isNullable()
                        && SqlUtil.isNullLiteral(pair.left, false)
                    ) {
                        throw newValidationError(
                            node,
                            RESOURCE.columnNotNullable(pair.right.getName())
                        )
                    }
                }
            }
        }
        for (operand in operands) {
            operand.validate(this, scope)
        }

        // validate that all row types have the same number of columns
        //  and that expressions in each column are compatible.
        // A values expression is turned into something that looks like
        // ROW(type00, type01,...), ROW(type11,...),...
        val rowCount: Int = operands.size()
        if (rowCount >= 2) {
            val firstRow: SqlCall = operands[0] as SqlCall
            val columnCount: Int = firstRow.operandCount()

            // 1. check that all rows have the same cols length
            for (operand in operands) {
                val thisRow: SqlCall = operand as SqlCall
                if (columnCount != thisRow.operandCount()) {
                    throw newValidationError(
                        node,
                        RESOURCE.incompatibleValueType(
                            SqlStdOperatorTable.VALUES.getName()
                        )
                    )
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for (col in 0 until columnCount) {
                val c: Int = col
                val type: RelDataType = typeFactory.leastRestrictive(
                    object : AbstractList<RelDataType?>() {
                        @Override
                        operator fun get(row: Int): RelDataType? {
                            val thisRow: SqlCall = operands[row] as SqlCall
                            return deriveType(scope, thisRow.operand(c))
                        }

                        @Override
                        fun size(): Int {
                            return rowCount
                        }
                    })
                    ?: throw newValidationError(
                        node,
                        RESOURCE.incompatibleValueType(
                            SqlStdOperatorTable.VALUES.getName()
                        )
                    )
            }
        }
    }

    @Override
    override fun validateDataType(dataType: SqlDataTypeSpec?) {
    }

    @Override
    override fun validateDynamicParam(dynamicParam: SqlDynamicParam?) {
    }

    /**
     * Throws a validator exception with access to the validator context.
     * The exception is determined when an instance is created.
     */
    private inner class ValidationError internal constructor(
        sqlNode: SqlNode,
        validatorException: Resources.ExInst<SqlValidatorException?>
    ) : Supplier<CalciteContextException?> {
        private val sqlNode: SqlNode
        private val validatorException: Resources.ExInst<SqlValidatorException?>

        init {
            this.sqlNode = sqlNode
            this.validatorException = validatorException
        }

        @Override
        fun get(): CalciteContextException {
            return newValidationError(sqlNode, validatorException)
        }
    }

    /**
     * Throws a validator exception with access to the validator context.
     * The exception is determined when the function is applied.
     */
    inner class ValidationErrorFunction :
        BiFunction<SqlNode?, Resources.ExInst<SqlValidatorException?>?, CalciteContextException?> {
        @Override
        fun apply(
            v0: SqlNode?, v1: Resources.ExInst<SqlValidatorException?>?
        ): CalciteContextException {
            return newValidationError(v0, v1)
        }
    }

    @Override
    override fun newValidationError(
        node: SqlNode?,
        e: Resources.ExInst<SqlValidatorException?>?
    ): CalciteContextException {
        assert(node != null)
        val pos: SqlParserPos = node.getParserPosition()
        return SqlUtil.newContextException(pos, e)
    }

    protected fun getWindowByName(
        id: SqlIdentifier,
        scope: SqlValidatorScope
    ): SqlWindow {
        var window: SqlWindow? = null
        if (id.isSimple()) {
            val name: String = id.getSimple()
            window = scope.lookupWindow(name)
        }
        if (window == null) {
            throw newValidationError(id, RESOURCE.windowNotFound(id.toString()))
        }
        return window
    }

    @Override
    fun resolveWindow(
        windowOrRef: SqlNode,
        scope: SqlValidatorScope
    ): SqlWindow {
        val window: SqlWindow
        window = if (windowOrRef is SqlIdentifier) {
            getWindowByName(windowOrRef as SqlIdentifier, scope)
        } else {
            windowOrRef as SqlWindow
        }
        while (true) {
            val refId: SqlIdentifier = window.getRefName() ?: break
            val refName: String = refId.getSimple()
            val refWindow: SqlWindow = scope.lookupWindow(refName)
                ?: throw newValidationError(refId, RESOURCE.windowNotFound(refName))
            window = window.overlay(refWindow, this)
        }
        return window
    }

    fun getOriginal(expr: SqlNode?): SqlNode? {
        var original: SqlNode? = originalExprs[expr]
        if (original == null) {
            original = expr
        }
        return original
    }

    fun setOriginal(expr: SqlNode?, original: SqlNode?) {
        // Don't overwrite the original original.
        originalExprs.putIfAbsent(expr, original)
    }

    @Nullable
    fun lookupFieldNamespace(rowType: RelDataType?, name: String?): SqlValidatorNamespace? {
        val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
        val field: RelDataTypeField = nameMatcher.field(rowType, name) ?: return null
        return FieldNamespace(this, field.getType())
    }

    @Override
    fun validateWindow(
        windowOrId: SqlNode,
        scope: SqlValidatorScope,
        @Nullable call: SqlCall
    ) {
        // Enable nested aggregates with window aggregates (OVER operator)
        inWindow = true
        val targetWindow: SqlWindow
        targetWindow = when (windowOrId.getKind()) {
            IDENTIFIER ->       // Just verify the window exists in this query.  It will validate
                // when the definition is processed
                getWindowByName(windowOrId as SqlIdentifier, scope)
            WINDOW -> windowOrId as SqlWindow
            else -> throw Util.unexpected(windowOrId.getKind())
        }
        requireNonNull(call) { "call is null when validating windowOrId $windowOrId" }
        assert(targetWindow.getWindowCall() == null)
        targetWindow.setWindowCall(call)
        targetWindow.validate(this, scope)
        targetWindow.setWindowCall(null)
        call.validate(this, scope)
        validateAggregateParams(call, null, null, null, scope)

        // Disable nested aggregates post validation
        inWindow = false
    }

    @Override
    override fun validateMatchRecognize(call: SqlCall) {
        val matchRecognize: SqlMatchRecognize = call as SqlMatchRecognize
        val scope: MatchRecognizeScope = getMatchRecognizeScope(matchRecognize) as MatchRecognizeScope
        val ns: MatchRecognizeNamespace = getNamespaceOrThrow(call).unwrap(MatchRecognizeNamespace::class.java)
        assert(ns.rowType == null)

        // rows per match
        val rowsPerMatch: SqlLiteral = matchRecognize.getRowsPerMatch()
        val allRows = (rowsPerMatch != null
                && rowsPerMatch.getValue()
                === SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS)
        val typeBuilder: RelDataTypeFactory.Builder = typeFactory.builder()

        // parse PARTITION BY column
        val partitionBy: SqlNodeList = matchRecognize.getPartitionList()
        if (partitionBy != null) {
            for (node in partitionBy) {
                val identifier: SqlIdentifier = node as SqlIdentifier
                identifier.validate(this, scope)
                val type: RelDataType? = deriveType(scope, identifier)
                val name: String = identifier.names.get(1)
                typeBuilder.add(name, type)
            }
        }

        // parse ORDER BY column
        val orderBy: SqlNodeList = matchRecognize.getOrderList()
        if (orderBy != null) {
            for (node in orderBy) {
                node.validate(this, scope)
                var identifier: SqlIdentifier
                identifier = if (node is SqlBasicCall) {
                    (node as SqlBasicCall).operand(0) as SqlIdentifier
                } else {
                    requireNonNull(
                        node
                    ) { "order by field is null. All fields: $orderBy" } as SqlIdentifier
                }
                if (allRows) {
                    val type: RelDataType? = deriveType(scope, identifier)
                    val name: String = identifier.names.get(1)
                    if (!typeBuilder.nameExists(name)) {
                        typeBuilder.add(name, type)
                    }
                }
            }
        }
        if (allRows) {
            val sqlNs: SqlValidatorNamespace = getNamespaceOrThrow(matchRecognize.getTableRef())
            val inputDataType: RelDataType = sqlNs.getRowType()
            for (fs in inputDataType.getFieldList()) {
                if (!typeBuilder.nameExists(fs.getName())) {
                    typeBuilder.add(fs)
                }
            }
        }

        // retrieve pattern variables used in pattern and subset
        val pattern: SqlNode = matchRecognize.getPattern()
        val visitor = PatternVarVisitor(scope)
        pattern.accept(visitor)
        val interval: SqlLiteral = matchRecognize.getInterval()
        if (interval != null) {
            interval.validate(this, scope)
            if ((interval as SqlIntervalLiteral).signum() < 0) {
                val intervalValue: String = interval.toValue()
                throw newValidationError(
                    interval,
                    RESOURCE.intervalMustBeNonNegative(
                        intervalValue ?: interval.toString()
                    )
                )
            }
            if (orderBy == null || orderBy.size() === 0) {
                throw newValidationError(
                    interval,
                    RESOURCE.cannotUseWithinWithoutOrderBy()
                )
            }
            val firstOrderByColumn: SqlNode = orderBy.get(0)
            val identifier: SqlIdentifier
            identifier = if (firstOrderByColumn is SqlBasicCall) {
                (firstOrderByColumn as SqlBasicCall).operand(0)
            } else {
                requireNonNull(firstOrderByColumn, "firstOrderByColumn") as SqlIdentifier
            }
            val firstOrderByColumnType: RelDataType? = deriveType(scope, identifier)
            if (firstOrderByColumnType.getSqlTypeName() !== SqlTypeName.TIMESTAMP) {
                throw newValidationError(
                    interval,
                    RESOURCE.firstColumnOfOrderByMustBeTimestamp()
                )
            }
            val expand: SqlNode = expand(interval, scope)
            val type: RelDataType? = deriveType(scope, expand)
            setValidatedNodeType(interval, type)
        }
        validateDefinitions(matchRecognize, scope)
        val subsets: SqlNodeList = matchRecognize.getSubsetList()
        if (subsets != null && subsets.size() > 0) {
            for (node in subsets) {
                val operands: List<SqlNode> = (node as SqlCall).getOperandList()
                val leftString: String = (operands[0] as SqlIdentifier).getSimple()
                if (scope.getPatternVars().contains(leftString)) {
                    throw newValidationError(
                        operands[0],
                        RESOURCE.patternVarAlreadyDefined(leftString)
                    )
                }
                scope.addPatternVar(leftString)
                for (right in operands[1] as SqlNodeList) {
                    val id: SqlIdentifier = right as SqlIdentifier
                    if (!scope.getPatternVars().contains(id.getSimple())) {
                        throw newValidationError(
                            id,
                            RESOURCE.unknownPattern(id.getSimple())
                        )
                    }
                    scope.addPatternVar(id.getSimple())
                }
            }
        }

        // validate AFTER ... SKIP TO
        val skipTo: SqlNode = matchRecognize.getAfter()
        if (skipTo is SqlCall) {
            val skipToCall: SqlCall = skipTo as SqlCall
            val id: SqlIdentifier = skipToCall.operand(0)
            if (!scope.getPatternVars().contains(id.getSimple())) {
                throw newValidationError(
                    id,
                    RESOURCE.unknownPattern(id.getSimple())
                )
            }
        }
        val measureColumns: List<Map.Entry<String, RelDataType>> = validateMeasure(matchRecognize, scope, allRows)
        for (c in measureColumns) {
            if (!typeBuilder.nameExists(c.getKey())) {
                typeBuilder.add(c.getKey(), c.getValue())
            }
        }
        val rowType: RelDataType = typeBuilder.build()
        if (matchRecognize.getMeasureList().size() === 0) {
            ns.setType(getNamespaceOrThrow(matchRecognize.getTableRef()).getRowType())
        } else {
            ns.setType(rowType)
        }
    }

    private fun validateMeasure(
        mr: SqlMatchRecognize,
        scope: MatchRecognizeScope, allRows: Boolean
    ): List<Map.Entry<String, RelDataType>> {
        val aliases: List<String> = ArrayList()
        val sqlNodes: List<SqlNode> = ArrayList()
        val measures: SqlNodeList = mr.getMeasureList()
        val fields: List<Map.Entry<String, RelDataType>> = ArrayList()
        for (measure in measures) {
            assert(measure is SqlCall)
            val alias = deriveAliasNonNull(measure, aliases.size())
            aliases.add(alias)
            var expand: SqlNode = expand(measure, scope)
            expand = navigationInMeasure(expand, allRows)
            setOriginal(expand, measure)
            inferUnknownTypes(unknownType, scope, expand)
            val type: RelDataType? = deriveType(scope, expand)
            setValidatedNodeType(measure, type)
            fields.add(Pair.of(alias, type))
            sqlNodes.add(
                SqlStdOperatorTable.AS.createCall(
                    SqlParserPos.ZERO, expand,
                    SqlIdentifier(alias, SqlParserPos.ZERO)
                )
            )
        }
        val list = SqlNodeList(sqlNodes, measures.getParserPosition())
        inferUnknownTypes(unknownType, scope, list)
        for (node in list) {
            validateExpr(node, scope)
        }
        mr.setOperand(SqlMatchRecognize.OPERAND_MEASURES, list)
        return fields
    }

    private fun navigationInMeasure(node: SqlNode, allRows: Boolean): SqlNode {
        var node: SqlNode = node
        val prefix: Set<String> = node.accept(PatternValidator(true))
        Util.discard(prefix)
        val ops: List<SqlNode> = (node as SqlCall).getOperandList()
        val defaultOp: SqlOperator = if (allRows) SqlStdOperatorTable.RUNNING else SqlStdOperatorTable.FINAL
        val op0: SqlNode = ops[0]
        if (!isRunningOrFinal(op0.getKind())
            || !allRows && op0.getKind() === SqlKind.RUNNING
        ) {
            val newNode: SqlNode = defaultOp.createCall(SqlParserPos.ZERO, op0)
            node = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, newNode, ops[1])
        }
        node = NavigationExpander().go(node)
        return node
    }

    private fun validateDefinitions(
        mr: SqlMatchRecognize,
        scope: MatchRecognizeScope
    ) {
        val aliases: Set<String> = catalogReader.nameMatcher().createSet()
        for (item in mr.getPatternDefList()) {
            val alias = alias(item)
            if (!aliases.add(alias)) {
                throw newValidationError(
                    item,
                    Static.RESOURCE.patternVarAlreadyDefined(alias)
                )
            }
            scope.addPatternVar(alias)
        }
        val sqlNodes: List<SqlNode> = ArrayList()
        for (item in mr.getPatternDefList()) {
            val alias = alias(item)
            var expand: SqlNode = expand(item, scope)
            expand = navigationInDefine(expand, alias)
            setOriginal(expand, item)
            inferUnknownTypes(booleanType, scope, expand)
            expand.validate(this, scope)

            // Some extra work need required here.
            // In PREV, NEXT, FINAL and LAST, only one pattern variable is allowed.
            sqlNodes.add(
                SqlStdOperatorTable.AS.createCall(
                    SqlParserPos.ZERO, expand,
                    SqlIdentifier(alias, SqlParserPos.ZERO)
                )
            )
            val type: RelDataType? = deriveType(scope, expand)
            if (!SqlTypeUtil.inBooleanFamily(type)) {
                throw newValidationError(expand, RESOURCE.condMustBeBoolean("DEFINE"))
            }
            setValidatedNodeType(item, type)
        }
        val list = SqlNodeList(sqlNodes, mr.getPatternDefList().getParserPosition())
        inferUnknownTypes(unknownType, scope, list)
        for (node in list) {
            validateExpr(node, scope)
        }
        mr.setOperand(SqlMatchRecognize.OPERAND_PATTERN_DEFINES, list)
    }

    fun validatePivot(pivot: SqlPivot) {
        val scope: PivotScope = requireNonNull(
            getJoinScope(pivot)
        ) { "joinScope for $pivot" } as PivotScope
        val ns: PivotNamespace = getNamespaceOrThrow(pivot).unwrap(PivotNamespace::class.java)
        assert(ns.rowType == null)

        // Given
        //   query PIVOT (agg1 AS a, agg2 AS b, ...
        //   FOR (axis1, ..., axisN)
        //   IN ((v11, ..., v1N) AS label1,
        //       (v21, ..., v2N) AS label2, ...))
        // the type is
        //   k1, ... kN, a_label1, b_label1, ..., a_label2, b_label2, ...
        // where k1, ... kN are columns that are not referenced as an argument to
        // an aggregate or as an axis.

        // Aggregates, e.g. "PIVOT (sum(x) AS sum_x, count(*) AS c)"
        val aggNames: List<Pair<String, RelDataType>> = ArrayList()
        pivot.forEachAgg { alias, call ->
            call.validate(this, scope)
            val type: RelDataType? = deriveType(scope, call)
            aggNames.add(Pair.of(alias, type))
            if (call !is SqlCall
                || (call as SqlCall).getOperator() !is SqlAggFunction
            ) {
                throw newValidationError(call, RESOURCE.pivotAggMalformed())
            }
        }

        // Axes, e.g. "FOR (JOB, DEPTNO)"
        val axisTypes: List<RelDataType> = ArrayList()
        val axisIdentifiers: List<SqlIdentifier> = ArrayList()
        for (axis in pivot.axisList) {
            val identifier: SqlIdentifier = axis as SqlIdentifier
            identifier.validate(this, scope)
            val type: RelDataType? = deriveType(scope, identifier)
            axisTypes.add(type)
            axisIdentifiers.add(identifier)
        }

        // Columns that have been seen as arguments to aggregates or as axes
        // do not appear in the output.
        val columnNames: Set<String> = pivot.usedColumnNames()
        val typeBuilder: RelDataTypeFactory.Builder = typeFactory.builder()
        scope.getChild().getRowType().getFieldList().forEach { field ->
            if (!columnNames.contains(field.getName())) {
                typeBuilder.add(field)
            }
        }

        // Values, e.g. "IN (('CLERK', 10) AS c10, ('MANAGER, 20) AS m20)"
        pivot.forEachNameValues { alias, nodeList ->
            if (nodeList.size() !== axisTypes.size()) {
                throw newValidationError(
                    nodeList,
                    RESOURCE.pivotValueArityMismatch(
                        nodeList.size(),
                        axisTypes.size()
                    )
                )
            }
            val typeChecker: SqlOperandTypeChecker = OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED
            Pair.forEach(axisIdentifiers, nodeList) { identifier, subNode ->
                subNode.validate(this, scope)
                typeChecker.checkOperandTypes(
                    SqlCallBinding(
                        this, scope,
                        SqlStdOperatorTable.EQUALS.createCall(
                            subNode.getParserPosition(), identifier, subNode
                        )
                    ),
                    true
                )
            }
            Pair.forEach(aggNames) { aggAlias, aggType ->
                typeBuilder.add(
                    if (aggAlias == null) alias else alias.toString() + "_" + aggAlias,
                    aggType
                )
            }
        }
        val rowType: RelDataType = typeBuilder.build()
        ns.setType(rowType)
    }

    fun validateUnpivot(unpivot: SqlUnpivot) {
        val scope: UnpivotScope = requireNonNull(getJoinScope(unpivot)) { "scope for $unpivot" } as UnpivotScope
        val ns: UnpivotNamespace = getNamespaceOrThrow(unpivot).unwrap(UnpivotNamespace::class.java)
        assert(ns.rowType == null)

        // Given
        //   query UNPIVOT ((measure1, ..., measureM)
        //   FOR (axis1, ..., axisN)
        //   IN ((c11, ..., c1M) AS (value11, ..., value1N),
        //       (c21, ..., c2M) AS (value21, ..., value2N), ...)
        // the type is
        //   k1, ... kN, axis1, ..., axisN, measure1, ..., measureM
        // where k1, ... kN are columns that are not referenced as an argument to
        // an aggregate or as an axis.

        // First, And make sure that each
        val measureCount: Int = unpivot.measureList.size()
        val axisCount: Int = unpivot.axisList.size()
        unpivot.forEachNameValues { nodeList, valueList ->
            // Make sure that each (ci1, ... ciM) list has the same arity as
            // (measure1, ..., measureM).
            if (nodeList.size() !== measureCount) {
                throw newValidationError(
                    nodeList,
                    RESOURCE.unpivotValueArityMismatch(
                        nodeList.size(),
                        measureCount
                    )
                )
            }

            // Make sure that each (vi1, ... viN) list has the same arity as
            // (axis1, ..., axisN).
            if (valueList != null && valueList.size() !== axisCount) {
                throw newValidationError(
                    valueList,
                    RESOURCE.unpivotValueArityMismatch(
                        valueList.size(),
                        axisCount
                    )
                )
            }

            // Make sure that each IN expression is a valid column from the input.
            nodeList.forEach { node -> deriveType(scope, node) }
        }

        // What columns from the input are not referenced by a column in the IN
        // list?
        val inputNs: SqlValidatorNamespace = Objects.requireNonNull(getNamespace(unpivot.query))
        val unusedColumnNames: Set<String> = catalogReader.nameMatcher().createSet()
        unusedColumnNames.addAll(inputNs.getRowType().getFieldNames())
        unusedColumnNames.removeAll(unpivot.usedColumnNames())

        // What columns will be present in the output row type?
        val columnNames: Set<String> = catalogReader.nameMatcher().createSet()
        columnNames.addAll(unusedColumnNames)

        // Gather the name and type of each measure.
        val measureNameTypes: List<Pair<String, RelDataType>> = ArrayList()
        Ord.forEach(unpivot.measureList) { measure, i ->
            val measureName: String = (measure as SqlIdentifier).getSimple()
            val types: List<RelDataType> = ArrayList()
            val nodes: List<SqlNode> = ArrayList()
            unpivot.forEachNameValues { nodeList, valueList ->
                val alias: SqlNode = nodeList.get(i)
                nodes.add(alias)
                types.add(deriveType(scope, alias))
            }
            val type0: RelDataType = typeFactory.leastRestrictive(types)
                ?: throw newValidationError(
                    nodes[0],
                    RESOURCE.unpivotCannotDeriveMeasureType(measureName)
                )
            val type: RelDataType = typeFactory.createTypeWithNullability(
                type0,
                unpivot.includeNulls || unpivot.measureList.size() > 1
            )
            setValidatedNodeType(measure, type)
            if (!columnNames.add(measureName)) {
                throw newValidationError(
                    measure,
                    RESOURCE.unpivotDuplicate(measureName)
                )
            }
            measureNameTypes.add(Pair.of(measureName, type))
        }

        // Gather the name and type of each axis.
        // Consider
        //   FOR (job, deptno)
        //   IN (a AS ('CLERK', 10),
        //       b AS ('ANALYST', 20))
        // There are two axes, (job, deptno), and so each value list ('CLERK', 10),
        // ('ANALYST', 20) must have arity two.
        //
        // The type of 'job' is derived as the least restrictive type of the values
        // ('CLERK', 'ANALYST'), namely VARCHAR(7). The derived type of 'deptno' is
        // the type of values (10, 20), namely INTEGER.
        val axisNameTypes: List<Pair<String, RelDataType>> = ArrayList()
        Ord.forEach(unpivot.axisList) { axis, i ->
            val axisName: String = (axis as SqlIdentifier).getSimple()
            val types: List<RelDataType> = ArrayList()
            unpivot.forEachNameValues { aliasList, valueList ->
                types.add(
                    if (valueList == null) typeFactory.createSqlType(
                        SqlTypeName.VARCHAR,
                        SqlUnpivot.aliasValue(aliasList).length()
                    ) else deriveType(scope, valueList.get(i))
                )
            }
            val type: RelDataType = typeFactory.leastRestrictive(types)
                ?: throw newValidationError(
                    axis,
                    RESOURCE.unpivotCannotDeriveAxisType(axisName)
                )
            setValidatedNodeType(axis, type)
            if (!columnNames.add(axisName)) {
                throw newValidationError(axis, RESOURCE.unpivotDuplicate(axisName))
            }
            axisNameTypes.add(Pair.of(axisName, type))
        }

        // Columns that have been seen as arguments to aggregates or as axes
        // do not appear in the output.
        val typeBuilder: RelDataTypeFactory.Builder = typeFactory.builder()
        scope.getChild().getRowType().getFieldList().forEach { field ->
            if (unusedColumnNames.contains(field.getName())) {
                typeBuilder.add(field)
            }
        }
        typeBuilder.addAll(axisNameTypes)
        typeBuilder.addAll(measureNameTypes)
        val rowType: RelDataType = typeBuilder.build()
        ns.setType(rowType)
    }

    /** Checks that all pattern variables within a function are the same,
     * and canonizes expressions such as `PREV(B.price)` to
     * `LAST(B.price, 0)`.  */
    private fun navigationInDefine(node: SqlNode, alpha: String): SqlNode {
        var node: SqlNode = node
        val prefix: Set<String> = node.accept(PatternValidator(false))
        Util.discard(prefix)
        node = NavigationExpander().go(node)
        node = NavigationReplacer(alpha).go(node)
        return node
    }

    @Override
    fun validateAggregateParams(
        aggCall: SqlCall,
        @Nullable filter: SqlNode?, @Nullable distinctList: SqlNodeList?,
        @Nullable orderList: SqlNodeList?, scope: SqlValidatorScope
    ) {
        // For "agg(expr)", expr cannot itself contain aggregate function
        // invocations.  For example, "SUM(2 * MAX(x))" is illegal; when
        // we see it, we'll report the error for the SUM (not the MAX).
        // For more than one level of nesting, the error which results
        // depends on the traversal order for validation.
        //
        // For a windowed aggregate "agg(expr)", expr can contain an aggregate
        // function. For example,
        //   SELECT AVG(2 * MAX(x)) OVER (PARTITION BY y)
        //   FROM t
        //   GROUP BY y
        // is legal. Only one level of nesting is allowed since non-windowed
        // aggregates cannot nest aggregates.

        // Store nesting level of each aggregate. If an aggregate is found at an invalid
        // nesting level, throw an assert.
        val a: AggFinder
        a = if (inWindow) {
            overFinder
        } else {
            aggOrOverFinder
        }
        for (param in aggCall.getOperandList()) {
            if (a.findAgg(param) != null) {
                throw newValidationError(aggCall, RESOURCE.nestedAggIllegal())
            }
        }
        if (filter != null) {
            if (a.findAgg(filter) != null) {
                throw newValidationError(filter, RESOURCE.aggregateInFilterIllegal())
            }
        }
        if (distinctList != null) {
            for (param in distinctList) {
                if (a.findAgg(param) != null) {
                    throw newValidationError(
                        aggCall,
                        RESOURCE.aggregateInWithinDistinctIllegal()
                    )
                }
            }
        }
        if (orderList != null) {
            for (param in orderList) {
                if (a.findAgg(param) != null) {
                    throw newValidationError(
                        aggCall,
                        RESOURCE.aggregateInWithinGroupIllegal()
                    )
                }
            }
        }
        val op: SqlAggFunction = aggCall.getOperator() as SqlAggFunction
        when (op.requiresGroupOrder()) {
            MANDATORY -> if (orderList == null || orderList.size() === 0) {
                throw newValidationError(
                    aggCall,
                    RESOURCE.aggregateMissingWithinGroupClause(op.getName())
                )
            }
            OPTIONAL -> {}
            IGNORED ->       // rewrite the order list to empty
                if (orderList != null) {
                    orderList.clear()
                }
            FORBIDDEN -> if (orderList != null && orderList.size() !== 0) {
                throw newValidationError(
                    aggCall,
                    RESOURCE.withinGroupClauseIllegalInAggregate(op.getName())
                )
            }
            else -> throw AssertionError(op)
        }
        if (op.isPercentile()) {
            assert(op.requiresGroupOrder() === Optionality.MANDATORY)
            assert(orderList != null)

            // Validate that percentile function have a single ORDER BY expression
            if (orderList.size() !== 1) {
                throw newValidationError(
                    orderList,
                    RESOURCE.orderByRequiresOneKey(op.getName())
                )
            }

            // Validate that the ORDER BY field is of NUMERIC type
            val node: SqlNode = orderList.get(0)
            assert(node != null)
            val type: RelDataType? = deriveType(scope, node)
            @Nullable val family: SqlTypeFamily = type.getSqlTypeName().getFamily()
            if (family == null
                || family.allowableDifferenceTypes().isEmpty()
            ) {
                throw newValidationError(
                    orderList,
                    RESOURCE.unsupportedTypeInOrderBy(
                        type.getSqlTypeName().getName(),
                        op.getName()
                    )
                )
            }
        }
    }

    @Override
    fun validateCall(
        call: SqlCall,
        scope: SqlValidatorScope
    ) {
        val operator: SqlOperator = call.getOperator()
        if (call.operandCount() === 0
            && operator.getSyntax() === SqlSyntax.FUNCTION_ID
            && !call.isExpanded()
            && !config.sqlConformance().allowNiladicParentheses()
        ) {
            // For example, "LOCALTIME()" is illegal. (It should be
            // "LOCALTIME", which would have been handled as a
            // SqlIdentifier.)
            throw handleUnresolvedFunction(
                call, operator,
                ImmutableList.of(), null
            )
        }
        val operandScope: SqlValidatorScope = scope.getOperandScope(call)
        if (operator is SqlFunction
            && ((operator as SqlFunction).getFunctionType()
                    === SqlFunctionCategory.MATCH_RECOGNIZE) && operandScope !is MatchRecognizeScope
        ) {
            throw newValidationError(
                call,
                Static.RESOURCE.functionMatchRecognizeOnly(call.toString())
            )
        }
        // Delegate validation to the operator.
        operator.validateCall(call, this, scope, operandScope)
    }

    /**
     * Validates that a particular feature is enabled. By default, all features
     * are enabled; subclasses may override this method to be more
     * discriminating.
     *
     * @param feature feature being used, represented as a resource instance
     * @param context parser position context for error reporting, or null if
     */
    protected fun validateFeature(
        feature: Feature,
        context: SqlParserPos?
    ) {
        // By default, do nothing except to verify that the resource
        // represents a real feature definition.
        assert(feature.getProperties().get("FeatureDefinition") != null)
    }

    fun expandSelectExpr(
        expr: SqlNode,
        scope: SelectScope?, select: SqlSelect
    ): SqlNode {
        val expander: Expander = SelectExpander(this, scope, select)
        val newExpr: SqlNode = expander.go(expr)
        if (expr !== newExpr) {
            setOriginal(newExpr, expr)
        }
        return newExpr
    }

    @Override
    override fun expand(expr: SqlNode?, scope: SqlValidatorScope?): SqlNode {
        val expander = Expander(this, scope)
        val newExpr: SqlNode = expander.go(expr)
        if (expr !== newExpr) {
            setOriginal(newExpr, expr)
        }
        return newExpr
    }

    fun expandGroupByOrHavingExpr(
        expr: SqlNode,
        scope: SqlValidatorScope?, select: SqlSelect, havingExpression: Boolean
    ): SqlNode {
        val expander: Expander = ExtendedExpander(
            this, scope, select, expr,
            havingExpression
        )
        val newExpr: SqlNode = expander.go(expr)
        if (expr !== newExpr) {
            setOriginal(newExpr, expr)
        }
        return newExpr
    }

    @Override
    override fun isSystemField(field: RelDataTypeField?): Boolean {
        return false
    }

    @Override
    override fun getFieldOrigins(sqlQuery: SqlNode): List<List<String>> {
        if (sqlQuery is SqlExplain) {
            return Collections.emptyList()
        }
        val rowType: RelDataType = getValidatedNodeType(sqlQuery)
        val fieldCount: Int = rowType.getFieldCount()
        if (!sqlQuery.isA(SqlKind.QUERY)) {
            return Collections.nCopies(fieldCount, null)
        }
        val list: List<List<String>> = ArrayList()
        for (i in 0 until fieldCount) {
            list.add(getFieldOrigin(sqlQuery, i))
        }
        return ImmutableNullableList.copyOf(list)
    }

    @Nullable
    private fun getFieldOrigin(sqlQuery: SqlNode, i: Int): List<String>? {
        return if (sqlQuery is SqlSelect) {
            val sqlSelect: SqlSelect = sqlQuery as SqlSelect
            val scope: SelectScope = getRawSelectScopeNonNull(sqlSelect)
            val selectList: List<SqlNode> = requireNonNull(
                scope.getExpandedSelectList()
            ) { "expandedSelectList for $scope" }
            val selectItem: SqlNode = stripAs(selectList[i])
            if (selectItem is SqlIdentifier) {
                val qualified: SqlQualified = scope.fullyQualify(selectItem as SqlIdentifier)
                var namespace: SqlValidatorNamespace = requireNonNull(
                    qualified.namespace
                ) { "namespace for $qualified" }
                val table: SqlValidatorTable = namespace.getTable() ?: return null
                val origin: List<String> = ArrayList(table.getQualifiedName())
                for (name in qualified.suffix()) {
                    namespace = namespace!!.lookupChild(name)
                    if (namespace == null) {
                        return null
                    }
                    origin.add(name)
                }
                return origin
            }
            null
        } else if (sqlQuery is SqlOrderBy) {
            getFieldOrigin((sqlQuery as SqlOrderBy).query, i)
        } else {
            null
        }
    }

    @Override
    override fun getParameterRowType(sqlQuery: SqlNode): RelDataType {
        // NOTE: We assume that bind variables occur in depth-first tree
        // traversal in the same order that they occurred in the SQL text.
        val types: List<RelDataType> = ArrayList()
        // NOTE: but parameters on fetch/offset would be counted twice
        // as they are counted in the SqlOrderBy call and the inner SqlSelect call
        val alreadyVisited: Set<SqlNode> = HashSet()
        sqlQuery.accept(
            object : SqlShuttle() {
                @Override
                fun visit(param: SqlDynamicParam?): SqlNode? {
                    if (alreadyVisited.add(param)) {
                        val type: RelDataType = getValidatedNodeType(param)
                        types.add(type)
                    }
                    return param
                }
            })
        return typeFactory.createStructType(
            types,
            object : AbstractList<String?>() {
                @Override
                operator fun get(index: Int): String {
                    return "?$index"
                }

                @Override
                fun size(): Int {
                    return types.size()
                }
            })
    }

    @Override
    override fun validateColumnListParams(
        function: SqlFunction?,
        argTypes: List<RelDataType?>?,
        operands: List<SqlNode?>?
    ) {
        throw UnsupportedOperationException()
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Common base class for DML statement namespaces.
     */
    class DmlNamespace protected constructor(
        validator: SqlValidatorImpl?, id: SqlNode?,
        enclosingNode: SqlNode?, parentScope: SqlValidatorScope?
    ) : IdentifierNamespace(validator, id, enclosingNode, parentScope)

    /**
     * Namespace for an INSERT statement.
     */
    private class InsertNamespace internal constructor(
        validator: SqlValidatorImpl?, node: SqlInsert?,
        enclosingNode: SqlNode?, parentScope: SqlValidatorScope?
    ) : DmlNamespace(validator, node.getTargetTable(), enclosingNode, parentScope) {
        private val node: SqlInsert

        init {
            this.node = requireNonNull(node, "node")
        }

        @Override
        @Nullable
        fun getNode(): SqlNode {
            return node
        }
    }

    /**
     * Namespace for an UPDATE statement.
     */
    private class UpdateNamespace internal constructor(
        validator: SqlValidatorImpl?, node: SqlUpdate?,
        enclosingNode: SqlNode?, parentScope: SqlValidatorScope?
    ) : DmlNamespace(validator, node.getTargetTable(), enclosingNode, parentScope) {
        private val node: SqlUpdate

        init {
            this.node = requireNonNull(node, "node")
        }

        @Override
        @Nullable
        fun getNode(): SqlNode {
            return node
        }
    }

    /**
     * Namespace for a DELETE statement.
     */
    private class DeleteNamespace internal constructor(
        validator: SqlValidatorImpl?, node: SqlDelete?,
        enclosingNode: SqlNode?, parentScope: SqlValidatorScope?
    ) : DmlNamespace(validator, node.getTargetTable(), enclosingNode, parentScope) {
        private val node: SqlDelete

        init {
            this.node = requireNonNull(node, "node")
        }

        @Override
        @Nullable
        fun getNode(): SqlNode {
            return node
        }
    }

    /**
     * Namespace for a MERGE statement.
     */
    private class MergeNamespace internal constructor(
        validator: SqlValidatorImpl?, node: SqlMerge?,
        enclosingNode: SqlNode?, parentScope: SqlValidatorScope?
    ) : DmlNamespace(validator, node.getTargetTable(), enclosingNode, parentScope) {
        private val node: SqlMerge

        init {
            this.node = requireNonNull(node, "node")
        }

        @Override
        @Nullable
        fun getNode(): SqlNode {
            return node
        }
    }

    /** Visitor that retrieves pattern variables defined.  */
    private class PatternVarVisitor internal constructor(scope: MatchRecognizeScope) : SqlVisitor<Void?> {
        private val scope: MatchRecognizeScope

        init {
            this.scope = scope
        }

        @Override
        fun visit(literal: SqlLiteral?): Void? {
            return null
        }

        @Override
        fun visit(call: SqlCall): Void? {
            for (i in 0 until call.getOperandList().size()) {
                call.getOperandList().get(i).accept(this)
            }
            return null
        }

        @Override
        fun visit(nodeList: SqlNodeList?): Void {
            throw Util.needToImplement(nodeList)
        }

        @Override
        fun visit(id: SqlIdentifier): Void? {
            Preconditions.checkArgument(id.isSimple())
            scope.addPatternVar(id.getSimple())
            return null
        }

        @Override
        fun visit(type: SqlDataTypeSpec?): Void {
            throw Util.needToImplement(type)
        }

        @Override
        fun visit(param: SqlDynamicParam?): Void {
            throw Util.needToImplement(param)
        }

        @Override
        fun visit(intervalQualifier: SqlIntervalQualifier?): Void {
            throw Util.needToImplement(intervalQualifier)
        }
    }

    /**
     * Visitor which derives the type of a given [SqlNode].
     *
     *
     * Each method must return the derived type. This visitor is basically a
     * single-use dispatcher; the visit is never recursive.
     */
    private inner class DeriveTypeVisitor internal constructor(scope: SqlValidatorScope) : SqlVisitor<RelDataType?> {
        private val scope: SqlValidatorScope

        init {
            this.scope = scope
        }

        @Override
        fun visit(literal: SqlLiteral): RelDataType {
            return literal.createSqlType(typeFactory)
        }

        @Override
        fun visit(call: SqlCall): RelDataType {
            val operator: SqlOperator = call.getOperator()
            return operator.deriveType(this@SqlValidatorImpl, scope, call)
        }

        @Override
        fun visit(nodeList: SqlNodeList?): RelDataType {
            // Operand is of a type that we can't derive a type for. If the
            // operand is of a peculiar type, such as a SqlNodeList, then you
            // should override the operator's validateCall() method so that it
            // doesn't try to validate that operand as an expression.
            throw Util.needToImplement(nodeList)
        }

        @Override
        fun visit(id: SqlIdentifier): RelDataType? {
            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            var id: SqlIdentifier = id
            val call: SqlCall? = makeNullaryCall(id)
            if (call != null) {
                return call.getOperator().validateOperands(
                    this@SqlValidatorImpl,
                    scope,
                    call
                )
            }
            var type: RelDataType? = null
            if (scope !is EmptyScope) {
                id = scope.fullyQualify(id).identifier
            }

            // Resolve the longest prefix of id that we can
            var i: Int
            i = id.names.size() - 1
            while (i > 0) {

                // REVIEW jvs 9-June-2005: The name resolution rules used
                // here are supposed to match SQL:2003 Part 2 Section 6.6
                // (identifier chain), but we don't currently have enough
                // information to get everything right.  In particular,
                // routine parameters are currently looked up via resolve;
                // we could do a better job if they were looked up via
                // resolveColumn.
                val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
                val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
                scope.resolve(id.names.subList(0, i), nameMatcher, false, resolved)
                if (resolved.count() === 1) {
                    // There's a namespace with the name we seek.
                    val resolve: Resolve = resolved.only()
                    type = resolve.rowType()
                    for (p in Util.skip(resolve.path.steps())) {
                        type = type.getFieldList().get(p.i).getType()
                    }
                    break
                }
                i--
            }

            // Give precedence to namespace found, unless there
            // are no more identifier components.
            if (type == null || id.names.size() === 1) {
                // See if there's a column with the name we seek in
                // precisely one of the namespaces in this scope.
                val colType: RelDataType = scope.resolveColumn(id.names.get(0), id)
                if (colType != null) {
                    type = colType
                }
                ++i
            }
            if (type == null) {
                val last: SqlIdentifier = id.getComponent(i - 1, i)
                throw newValidationError(
                    last,
                    RESOURCE.unknownIdentifier(last.toString())
                )
            }

            // Resolve rest of identifier
            while (i < id.names.size()) {
                var name: String = id.names.get(i)
                val field: RelDataTypeField?
                if (name.equals("")) {
                    // The wildcard "*" is represented as an empty name. It never
                    // resolves to a field.
                    name = "*"
                    field = null
                } else {
                    val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
                    field = nameMatcher.field(type, name)
                }
                if (field == null) {
                    throw newValidationError(
                        id.getComponent(i),
                        RESOURCE.unknownField(name)
                    )
                }
                type = field.getType()
                i++
            }
            type = SqlTypeUtil.addCharsetAndCollation(
                type,
                getTypeFactory()
            )
            return type
        }

        @Override
        fun visit(dataType: SqlDataTypeSpec): RelDataType {
            // Q. How can a data type have a type?
            // A. When it appears in an expression. (Say as the 2nd arg to the
            //    CAST operator.)
            validateDataType(dataType)
            return dataType.deriveType(this@SqlValidatorImpl)
        }

        @Override
        fun visit(param: SqlDynamicParam?): RelDataType {
            return unknownType
        }

        @Override
        fun visit(intervalQualifier: SqlIntervalQualifier?): RelDataType {
            return typeFactory.createSqlIntervalType(intervalQualifier)
        }
    }

    /**
     * Converts an expression into canonical form by fully-qualifying any
     * identifiers.
     */
    private class Expander internal constructor(protected val validator: SqlValidatorImpl, scope: SqlValidatorScope?) :
        SqlScopedShuttle(scope) {
        fun go(root: SqlNode?): SqlNode {
            return requireNonNull(
                root.accept(this)
            ) { "$this returned null for $root" }
        }

        @Override
        @Nullable
        override fun visit(id: SqlIdentifier): SqlNode {
            // First check for builtin functions which don't have
            // parentheses, like "LOCALTIME".
            val call: SqlCall? = validator.makeNullaryCall(id)
            if (call != null) {
                return call.accept(this)
            }
            val fqId: SqlIdentifier = getScope().fullyQualify(id).identifier
            val expandedExpr: SqlNode = expandDynamicStar(id, fqId)
            validator.setOriginal(expandedExpr, id)
            return expandedExpr
        }

        @Override
        protected override fun visitScoped(call: SqlCall): SqlNode {
            when (call.getKind()) {
                SCALAR_QUERY, CURRENT_VALUE, NEXT_VALUE, WITH -> return call
                else -> {}
            }
            // Only visits arguments which are expressions. We don't want to
            // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
            val argHandler = CallCopyingArgHandler(call, false)
            call.getOperator().acceptCall(this, call, true, argHandler)
            val result: SqlNode = argHandler.result()
            validator.setOriginal(result, call)
            return result
        }

        protected fun expandDynamicStar(id: SqlIdentifier, fqId: SqlIdentifier): SqlNode {
            return if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
                && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))
            ) {
                // Convert a column ref into ITEM(*, 'col_name')
                // for a dynamic star field in dynTable's rowType.
                SqlBasicCall(
                    SqlStdOperatorTable.ITEM,
                    ImmutableList.of(
                        fqId,
                        SqlLiteral.createCharString(
                            Util.last(id.names),
                            id.getParserPosition()
                        )
                    ),
                    id.getParserPosition()
                )
            } else fqId
        }
    }

    /**
     * Shuttle which walks over an expression in the ORDER BY clause, replacing
     * usages of aliases with the underlying expression.
     */
    internal inner class OrderExpressionExpander(select: SqlSelect, root: SqlNode) :
        SqlScopedShuttle(getOrderScope(select)) {
        private val aliasList: List<String>
        private val select: SqlSelect
        private val root: SqlNode

        init {
            this.select = select
            this.root = root
            aliasList = getNamespaceOrThrow(select).getRowType().getFieldNames()
        }

        fun go(): SqlNode {
            return requireNonNull(
                root.accept(this)
            ) { "OrderExpressionExpander returned null for $root" }
        }

        @Override
        @Nullable
        override fun visit(literal: SqlLiteral): SqlNode {
            // Ordinal markers, e.g. 'select a, b from t order by 2'.
            // Only recognize them if they are the whole expression,
            // and if the dialect permits.
            if (literal === root && config.sqlConformance().isSortByOrdinal()) {
                when (literal.getTypeName()) {
                    DECIMAL, DOUBLE -> {
                        val intValue: Int = literal.intValue(false)
                        if (intValue >= 0) {
                            if (intValue < 1 || intValue > aliasList.size()) {
                                throw newValidationError(
                                    literal, RESOURCE.orderByOrdinalOutOfRange()
                                )
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            val ordinal = intValue - 1
                            return nthSelectItem(ordinal, literal.getParserPosition())
                        }
                    }
                    else -> {}
                }
            }
            return super.visit(literal)
        }

        /**
         * Returns the `ordinal`th item in the select list.
         */
        private fun nthSelectItem(ordinal: Int, pos: SqlParserPos): SqlNode {
            // TODO: Don't expand the list every time. Maybe keep an expanded
            // version of each expression -- select lists and identifiers -- in
            // the validator.
            val expandedSelectList: SqlNodeList = expandStar(
                SqlNonNullableAccessors.getSelectList(select),
                select,
                false
            )
            var expr: SqlNode = expandedSelectList.get(ordinal)
            expr = stripAs(expr)
            if (expr is SqlIdentifier) {
                expr = getScope().fullyQualify(expr as SqlIdentifier).identifier
            }

            // Create a copy of the expression with the position of the order
            // item.
            return expr.clone(pos)
        }

        @Override
        override fun visit(id: SqlIdentifier): SqlNode {
            // Aliases, e.g. 'select a as x, b from t order by x'.
            if (id.isSimple()
                && config.sqlConformance().isSortByAlias()
            ) {
                val alias: String = id.getSimple()
                val selectNs: SqlValidatorNamespace = getNamespaceOrThrow(select)
                val rowType: RelDataType = selectNs.getRowTypeSansSystemColumns()
                val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
                val field: RelDataTypeField = nameMatcher.field(rowType, alias)
                if (field != null) {
                    return nthSelectItem(
                        field.getIndex(),
                        id.getParserPosition()
                    )
                }
            }

            // No match. Return identifier unchanged.
            return getScope().fullyQualify(id).identifier
        }

        @Override
        @Nullable
        protected override fun visitScoped(call: SqlCall): SqlNode {
            // Don't attempt to expand sub-queries. We haven't implemented
            // these yet.
            return if (call is SqlSelect) {
                call
            } else super.visitScoped(call)
        }
    }

    /**
     * Converts an expression into canonical form by fully-qualifying any
     * identifiers. For common columns in USING, it will be converted to
     * COALESCE(A.col, B.col) AS col.
     */
    internal class SelectExpander(
        validator: SqlValidatorImpl, scope: SelectScope?,
        select: SqlSelect
    ) : Expander(validator, scope) {
        val select: SqlSelect

        init {
            this.select = select
        }

        @Override
        @Nullable
        override fun visit(id: SqlIdentifier): SqlNode {
            val node: SqlNode = expandCommonColumn(select, id, getScope() as SelectScope, validator)
            return if (node !== id) {
                node
            } else {
                super.visit(id)
            }
        }
    }

    /**
     * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing
     * usages of aliases or ordinals with the underlying expression.
     */
    internal class ExtendedExpander(
        validator: SqlValidatorImpl, scope: SqlValidatorScope?,
        select: SqlSelect, root: SqlNode, havingExpr: Boolean
    ) : Expander(validator, scope) {
        val select: SqlSelect
        val root: SqlNode
        val havingExpr: Boolean

        init {
            this.select = select
            this.root = root
            this.havingExpr = havingExpr
        }

        @Override
        @Nullable
        override fun visit(id: SqlIdentifier): SqlNode? {
            if (id.isSimple()
                && if (havingExpr) validator.config().sqlConformance().isHavingAlias() else validator.config()
                    .sqlConformance().isGroupByAlias()
            ) {
                val name: String = id.getSimple()
                var expr: SqlNode? = null
                val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
                var n = 0
                for (s in SqlNonNullableAccessors.getSelectList(select)) {
                    val alias: String = SqlValidatorUtil.getAlias(s, -1)
                    if (alias != null && nameMatcher.matches(alias, name)) {
                        expr = s
                        n++
                    }
                }
                if (n == 0) {
                    return super.visit(id)
                } else if (n > 1) {
                    // More than one column has this alias.
                    throw validator.newValidationError(
                        id,
                        RESOURCE.columnAmbiguous(name)
                    )
                }
                if (havingExpr && validator.isAggregate(root)) {
                    return super.visit(id)
                }
                expr = stripAs(expr)
                if (expr is SqlIdentifier) {
                    val sid: SqlIdentifier? = expr as SqlIdentifier?
                    val fqId: SqlIdentifier = getScope().fullyQualify(sid).identifier
                    expr = expandDynamicStar(sid, fqId)
                }
                return expr
            }
            if (id.isSimple()) {
                val scope: SelectScope = validator.getRawSelectScope(select)
                val node: SqlNode = expandCommonColumn(select, id, scope, validator)
                if (node !== id) {
                    return node
                }
            }
            return super.visit(id)
        }

        @Override
        @Nullable
        override fun visit(literal: SqlLiteral): SqlNode {
            if (havingExpr || !validator.config().sqlConformance().isGroupByOrdinal()) {
                return super.visit(literal)
            }
            var isOrdinalLiteral = literal === root
            when (root.getKind()) {
                GROUPING_SETS, ROLLUP, CUBE -> if (root is SqlBasicCall) {
                    val operandList: List<SqlNode> = (root as SqlBasicCall).getOperandList()
                    for (node in operandList) {
                        if (node.equals(literal)) {
                            isOrdinalLiteral = true
                            break
                        }
                    }
                }
                else -> {}
            }
            if (isOrdinalLiteral) {
                when (literal.getTypeName()) {
                    DECIMAL, DOUBLE -> {
                        val intValue: Int = literal.intValue(false)
                        if (intValue >= 0) {
                            if (intValue < 1 || intValue > SqlNonNullableAccessors.getSelectList(select).size()) {
                                throw validator.newValidationError(
                                    literal,
                                    RESOURCE.orderByOrdinalOutOfRange()
                                )
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            val ordinal = intValue - 1
                            return SqlUtil.stripAs(SqlNonNullableAccessors.getSelectList(select).get(ordinal))
                        }
                    }
                    else -> {}
                }
            }
            return super.visit(literal)
        }
    }

    /** Information about an identifier in a particular scope.  */
    protected class IdInfo(scope: SqlValidatorScope?, id: SqlIdentifier) {
        val scope: SqlValidatorScope?
        val id: SqlIdentifier

        init {
            this.scope = scope
            this.id = id
        }
    }

    /**
     * Utility object used to maintain information about the parameters in a
     * function call.
     */
    protected class FunctionParamInfo {
        /**
         * Maps a cursor (based on its position relative to other cursor
         * parameters within a function call) to the SELECT associated with the
         * cursor.
         */
        val cursorPosToSelectMap: Map<Integer, SqlSelect>

        /**
         * Maps a column list parameter to the parent cursor parameter it
         * references. The parameters are id'd by their names.
         */
        val columnListParamToParentCursorMap: Map<String, String>

        init {
            cursorPosToSelectMap = HashMap()
            columnListParamToParentCursorMap = HashMap()
        }
    }

    /**
     * Modify the nodes in navigation function
     * such as FIRST, LAST, PREV AND NEXT.
     */
    private class NavigationModifier : SqlShuttle() {
        fun go(node: SqlNode): SqlNode {
            return requireNonNull(
                node.accept(this)
            ) { "NavigationModifier returned for $node" }
        }
    }

    /**
     * Shuttle that expands navigation expressions in a MATCH_RECOGNIZE clause.
     *
     *
     * Examples:
     *
     *
     *  * `PREV(A.price + A.amount)`
     * `PREV(A.price) + PREV(A.amount)`
     *
     *  * `FIRST(A.price * 2)`  `FIRST(A.PRICE) * 2`
     *
     */
    private class NavigationExpander @JvmOverloads internal constructor(
        @Nullable operator: SqlOperator? = null,
        @Nullable offset: SqlNode? = null
    ) : NavigationModifier() {
        @Nullable
        val op: SqlOperator?

        @Nullable
        val offset: SqlNode?

        init {
            this.offset = offset
            op = operator
        }

        @Override
        @Nullable
        fun visit(call: SqlCall): SqlNode {
            val kind: SqlKind = call.getKind()
            val operands: List<SqlNode> = call.getOperandList()
            val newOperands: List<SqlNode> = ArrayList()
            if (call.getFunctionQuantifier() != null
                && call.getFunctionQuantifier().getValue() === SqlSelectKeyword.DISTINCT
            ) {
                val pos: SqlParserPos = call.getParserPosition()
                throw SqlUtil.newContextException(
                    pos,
                    Static.RESOURCE.functionQuantifierNotAllowed(call.toString())
                )
            }
            if (isLogicalNavigation(kind) || isPhysicalNavigation(kind)) {
                var inner: SqlNode = operands[0]
                var offset: SqlNode = operands[1]

                // merge two straight prev/next, update offset
                if (isPhysicalNavigation(kind)) {
                    val innerKind: SqlKind = inner.getKind()
                    if (isPhysicalNavigation(innerKind)) {
                        val innerOperands: List<SqlNode> = (inner as SqlCall).getOperandList()
                        val innerOffset: SqlNode = innerOperands[1]
                        val newOperator: SqlOperator =
                            if (innerKind === kind) SqlStdOperatorTable.PLUS else SqlStdOperatorTable.MINUS
                        offset = newOperator.createCall(
                            SqlParserPos.ZERO,
                            offset, innerOffset
                        )
                        inner = call.getOperator().createCall(
                            SqlParserPos.ZERO,
                            innerOperands[0], offset
                        )
                    }
                }
                var newInnerNode: SqlNode = inner.accept(NavigationExpander(call.getOperator(), offset))
                if (op != null) {
                    newInnerNode = op.createCall(
                        SqlParserPos.ZERO, newInnerNode,
                        this.offset
                    )
                }
                return newInnerNode
            }
            return if (operands.size() > 0) {
                for (node in operands) {
                    if (node != null) {
                        var newNode: SqlNode = node.accept(NavigationExpander())
                        if (op != null) {
                            newNode = op.createCall(SqlParserPos.ZERO, newNode, offset)
                        }
                        newOperands.add(newNode)
                    } else {
                        newOperands.add(null)
                    }
                }
                call.getOperator().createCall(SqlParserPos.ZERO, newOperands)
            } else {
                if (op == null) {
                    call
                } else {
                    op.createCall(SqlParserPos.ZERO, call, offset)
                }
            }
        }

        @Override
        fun visit(id: SqlIdentifier): SqlNode {
            return if (op == null) {
                id
            } else {
                op.createCall(SqlParserPos.ZERO, id, offset)
            }
        }
    }

    /**
     * Shuttle that replaces `A as A.price > PREV(B.price)` with
     * `PREV(A.price, 0) > LAST(B.price, 0)`.
     *
     *
     * Replacing `A.price` with `PREV(A.price, 0)` makes the
     * implementation of
     * [RexVisitor.visitPatternFieldRef] more unified.
     * Otherwise, it's difficult to implement this method. If it returns the
     * specified field, then the navigation such as `PREV(A.price, 1)`
     * becomes impossible; if not, then comparisons such as
     * `A.price > PREV(A.price, 1)` become meaningless.
     */
    private class NavigationReplacer internal constructor(private val alpha: String) : NavigationModifier() {
        @Override
        @Nullable
        fun visit(call: SqlCall): SqlNode {
            val kind: SqlKind = call.getKind()
            if (isLogicalNavigation(kind)
                || isAggregation(kind)
                || isRunningOrFinal(kind)
            ) {
                return call
            }
            when (kind) {
                PREV -> {
                    val operands: List<SqlNode> = call.getOperandList()
                    if (operands[0] is SqlIdentifier) {
                        val name: String = (operands[0] as SqlIdentifier).names.get(0)
                        return if (name.equals(alpha)) call else SqlStdOperatorTable.LAST.createCall(
                            SqlParserPos.ZERO,
                            operands
                        )
                    }
                }
                else -> {}
            }
            return super.visit(call)
        }

        @Override
        fun visit(id: SqlIdentifier): SqlNode {
            if (id.isSimple()) {
                return id
            }
            val operator: SqlOperator =
                if (id.names.get(0).equals(alpha)) SqlStdOperatorTable.PREV else SqlStdOperatorTable.LAST
            return operator.createCall(
                SqlParserPos.ZERO, id,
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)
            )
        }
    }

    /** Validates that within one navigation function, the pattern var is the
     * same.  */
    private inner class PatternValidator @JvmOverloads internal constructor(
        private val isMeasure: Boolean, var firstLastCount: Int = 0, var prevNextCount: Int = 0,
        var aggregateCount: Int = 0
    ) : SqlBasicVisitor<Set<String?>?>() {
        @Override
        fun visit(call: SqlCall): Set<String> {
            var isSingle = false
            val vars: Set<String> = HashSet()
            val kind: SqlKind = call.getKind()
            val operands: List<SqlNode> = call.getOperandList()
            if (isSingleVarRequired(kind)) {
                isSingle = true
                if (isPhysicalNavigation(kind)) {
                    if (isMeasure) {
                        throw newValidationError(
                            call,
                            Static.RESOURCE.patternPrevFunctionInMeasure(call.toString())
                        )
                    }
                    if (firstLastCount != 0) {
                        throw newValidationError(
                            call,
                            Static.RESOURCE.patternPrevFunctionOrder(call.toString())
                        )
                    }
                    prevNextCount++
                } else if (isLogicalNavigation(kind)) {
                    if (firstLastCount != 0) {
                        throw newValidationError(
                            call,
                            Static.RESOURCE.patternPrevFunctionOrder(call.toString())
                        )
                    }
                    firstLastCount++
                } else if (isAggregation(kind)) {
                    // cannot apply aggregation in PREV/NEXT, FIRST/LAST
                    if (firstLastCount != 0 || prevNextCount != 0) {
                        throw newValidationError(
                            call,
                            Static.RESOURCE.patternAggregationInNavigation(call.toString())
                        )
                    }
                    if (kind === SqlKind.COUNT && call.getOperandList().size() > 1) {
                        throw newValidationError(
                            call,
                            Static.RESOURCE.patternCountFunctionArg()
                        )
                    }
                    aggregateCount++
                }
            }
            if (isRunningOrFinal(kind) && !isMeasure) {
                throw newValidationError(
                    call,
                    Static.RESOURCE.patternRunningFunctionInDefine(call.toString())
                )
            }
            for (node in operands) {
                if (node != null) {
                    vars.addAll(
                        requireNonNull(
                            node.accept(
                                PatternValidator(
                                    isMeasure, firstLastCount, prevNextCount,
                                    aggregateCount
                                )
                            )
                        ) { "node.accept(PatternValidator) for node $node" })
                }
            }
            if (isSingle) {
                when (kind) {
                    COUNT -> if (vars.size() > 1) {
                        throw newValidationError(
                            call,
                            Static.RESOURCE.patternCountFunctionArg()
                        )
                    }
                    else -> if (operands.size() === 0 || operands[0] !is SqlCall
                        || (operands[0] as SqlCall).getOperator() !== SqlStdOperatorTable.CLASSIFIER
                    ) {
                        if (vars.isEmpty()) {
                            throw newValidationError(
                                call,
                                Static.RESOURCE.patternFunctionNullCheck(call.toString())
                            )
                        }
                        if (vars.size() !== 1) {
                            throw newValidationError(
                                call,
                                Static.RESOURCE.patternFunctionVariableCheck(call.toString())
                            )
                        }
                    }
                }
            }
            return vars
        }

        @Override
        fun visit(identifier: SqlIdentifier): Set<String> {
            val check = prevNextCount > 0 || firstLastCount > 0 || aggregateCount > 0
            val vars: Set<String> = HashSet()
            if (identifier.names.size() > 1 && check) {
                vars.add(identifier.names.get(0))
            }
            return vars
        }

        @Override
        fun visit(literal: SqlLiteral?): Set<String> {
            return ImmutableSet.of()
        }

        @Override
        fun visit(qualifier: SqlIntervalQualifier?): Set<String> {
            return ImmutableSet.of()
        }

        @Override
        fun visit(type: SqlDataTypeSpec?): Set<String> {
            return ImmutableSet.of()
        }

        @Override
        fun visit(param: SqlDynamicParam?): Set<String> {
            return ImmutableSet.of()
        }
    }

    /** Permutation of fields in NATURAL JOIN or USING.  */
    private inner class Permute internal constructor(from: SqlNode, offset: Int) {
        val sources: List<ImmutableIntList>? = null
        var rowType: RelDataType? = null
        var trivial = false

        init {
            when (from.getKind()) {
                JOIN -> {
                    val join: SqlJoin = from as SqlJoin
                    val left: Permute = Permute(join.getLeft(), offset)
                    val fieldCount: Int = getValidatedNodeType(join.getLeft()).getFieldList().size()
                    val right: Permute = Permute(join.getRight(), offset + fieldCount)
                    val names = usingNames(join)
                    val sources: List<ImmutableIntList> = ArrayList()
                    val sourceSet: Set<ImmutableIntList> = HashSet()
                    val b: RelDataTypeFactory.Builder = typeFactory.builder()
                    if (names != null) {
                        for (name in names) {
                            val f: RelDataTypeField? = left.field(name)
                            val source: ImmutableIntList = left.sources!![f.getIndex()]
                            sourceSet.add(source)
                            val f2: RelDataTypeField? = right.field(name)
                            val source2: ImmutableIntList = right.sources!![f2.getIndex()]
                            sourceSet.add(source2)
                            sources.add(source.appendAll(source2))
                            val nullable = ((f.getType().isNullable()
                                    || join.getJoinType().generatesNullsOnLeft())
                                    && (f2.getType().isNullable()
                                    || join.getJoinType().generatesNullsOnRight()))
                            b.add(f).nullable(nullable)
                        }
                    }
                    for (f in left.rowType.getFieldList()) {
                        val source: ImmutableIntList = left.sources!![f.getIndex()]
                        if (sourceSet.add(source)) {
                            sources.add(source)
                            b.add(f)
                        }
                    }
                    for (f in right.rowType.getFieldList()) {
                        val source: ImmutableIntList = right.sources!![f.getIndex()]
                        if (sourceSet.add(source)) {
                            sources.add(source)
                            b.add(f)
                        }
                    }
                    rowType = b.build()
                    this.sources = ImmutableList.copyOf(sources)
                    trivial = (left.trivial
                            && right.trivial
                            && (names == null || names.isEmpty()))
                }
                else -> {
                    rowType = getValidatedNodeType(from)
                    sources = Functions.generate(
                        rowType.getFieldCount()
                    ) { i -> ImmutableIntList.of(offset + i) }
                    trivial = true
                }
            }
        }

        private fun field(name: String): RelDataTypeField? {
            val field: RelDataTypeField = catalogReader.nameMatcher().field(rowType, name)
            assert(field != null) { "field $name was not found in $rowType" }
            return field
        }

        /** Moves fields according to the permutation.  */
        fun permute(
            selectItems: List<SqlNode?>,
            fields: List<Map.Entry<String?, RelDataType?>?>
        ) {
            if (trivial) {
                return
            }
            val oldSelectItems: List<SqlNode> = ImmutableList.copyOf(selectItems)
            selectItems.clear()
            val oldFields: List<Map.Entry<String, RelDataType>> = ImmutableList.copyOf(fields)
            fields.clear()
            for (source in sources) {
                val p0: Int = source.get(0)
                val field: Map.Entry<String, RelDataType> = oldFields[p0]
                val name: String = field.getKey()
                var type: RelDataType = field.getValue()
                var selectItem: SqlNode = oldSelectItems[p0]
                for (p1 in Util.skip(source)) {
                    val field1: Map.Entry<String, RelDataType> = oldFields[p1]
                    val selectItem1: SqlNode = oldSelectItems[p1]
                    val type1: RelDataType = field1.getValue()
                    // output is nullable only if both inputs are
                    val nullable = type.isNullable() && type1.isNullable()
                    val currentType: RelDataType = type
                    val type2: RelDataType = requireNonNull(
                        SqlTypeUtil.leastRestrictiveForComparison(typeFactory, type, type1)
                    ) { "leastRestrictiveForComparison for types $currentType and $type1" }
                    selectItem = SqlStdOperatorTable.AS.createCall(
                        SqlParserPos.ZERO,
                        SqlStdOperatorTable.COALESCE.createCall(
                            SqlParserPos.ZERO,
                            maybeCast(selectItem, type, type2),
                            maybeCast(selectItem1, type1, type2)
                        ),
                        SqlIdentifier(name, SqlParserPos.ZERO)
                    )
                    type = typeFactory.createTypeWithNullability(type2, nullable)
                }
                fields.add(Pair.of(name, type))
                selectItems.add(selectItem)
            }
        }
    }
    //~ Enums ------------------------------------------------------------------
    /**
     * Validation status.
     */
    enum class Status {
        /**
         * Validation has not started for this scope.
         */
        UNVALIDATED,

        /**
         * Validation is in progress for this scope.
         */
        IN_PROGRESS,

        /**
         * Validation has completed (perhaps unsuccessfully).
         */
        VALID
    }

    /** Allows [.clauseScopes] to have multiple values per SELECT.  */
    private enum class Clause {
        WHERE, GROUP_BY, SELECT, ORDER, CURSOR
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val TRACER: Logger = CalciteTrace.PARSER_LOGGER

        /**
         * Alias generated for the source table when rewriting UPDATE to MERGE.
         */
        const val UPDATE_SRC_ALIAS = "SYS\$SRC"

        /**
         * Alias generated for the target table when rewriting UPDATE to MERGE if no
         * alias was specified by the user.
         */
        const val UPDATE_TGT_ALIAS = "SYS\$TGT"

        /**
         * Alias prefix generated for source columns when rewriting UPDATE to MERGE.
         */
        const val UPDATE_ANON_PREFIX = "SYS\$ANON"
        private fun expandExprFromJoin(
            join: SqlJoin, identifier: SqlIdentifier,
            @Nullable scope: SelectScope
        ): SqlNode {
            if (join.getConditionType() !== JoinConditionType.USING) {
                return identifier
            }
            for (name in SqlIdentifier.simpleNames(getCondition(join) as SqlNodeList?)) {
                if (identifier.getSimple().equals(name)) {
                    val qualifiedNode: List<SqlNode> = ArrayList()
                    for (child in requireNonNull(scope, "scope").children) {
                        if (child.namespace.getRowType()
                                .getFieldNames().indexOf(name) >= 0
                        ) {
                            val exp = SqlIdentifier(
                                ImmutableList.of(child.name, name),
                                identifier.getParserPosition()
                            )
                            qualifiedNode.add(exp)
                        }
                    }
                    assert(qualifiedNode.size() === 2)
                    return SqlStdOperatorTable.AS.createCall(
                        SqlParserPos.ZERO,
                        SqlStdOperatorTable.COALESCE.createCall(
                            SqlParserPos.ZERO,
                            qualifiedNode[0],
                            qualifiedNode[1]
                        ),
                        SqlIdentifier(name, SqlParserPos.ZERO)
                    )
                }
            }

            // Only need to try to expand the expr from the left input of join
            // since it is always left-deep join.
            val node: SqlNode = join.getLeft()
            return if (node is SqlJoin) {
                expandExprFromJoin(node as SqlJoin, identifier, scope)
            } else {
                identifier
            }
        }

        private fun expandCommonColumn(
            sqlSelect: SqlSelect,
            selectItem: SqlNode, @Nullable scope: SelectScope, validator: SqlValidatorImpl
        ): SqlNode {
            if (selectItem !is SqlIdentifier) {
                return selectItem
            }
            val from: SqlNode = sqlSelect.getFrom() as? SqlJoin ?: return selectItem
            val identifier: SqlIdentifier = selectItem as SqlIdentifier
            if (!identifier.isSimple()) {
                if (!validator.config().sqlConformance().allowQualifyingCommonColumn()) {
                    validateQualifiedCommonColumn(from as SqlJoin, identifier, scope, validator)
                }
                return selectItem
            }
            return expandExprFromJoin(from as SqlJoin, identifier, scope)
        }

        private fun validateQualifiedCommonColumn(
            join: SqlJoin,
            identifier: SqlIdentifier, @Nullable scope: SelectScope, validator: SqlValidatorImpl
        ) {
            val names = validator.usingNames(join)
                ?: // Not USING or NATURAL.
                return
            requireNonNull(scope, "scope")
            // First we should make sure that the first component is the table name.
            // Then check whether the qualified identifier contains common column.
            for (child in scope.children) {
                if (Objects.equals(child.name, identifier.getComponent(0).toString())) {
                    if (names.contains(identifier.getComponent(1).toString())) {
                        throw validator.newValidationError(
                            identifier,
                            RESOURCE.disallowsQualifyingCommonColumn(identifier.toString())
                        )
                    }
                }
            }

            // Only need to try to validate the expr from the left input of join
            // since it is always left-deep join.
            val node: SqlNode = join.getLeft()
            if (node is SqlJoin) {
                validateQualifiedCommonColumn(node as SqlJoin, identifier, scope, validator)
            }
        }

        private fun findAllValidUdfNames(
            names: List<String>,
            validator: SqlValidator,
            result: Collection<SqlMoniker>
        ) {
            val objNames: List<SqlMoniker> = ArrayList()
            SqlValidatorUtil.getSchemaObjectMonikers(
                validator.getCatalogReader(),
                names,
                objNames
            )
            for (objName in objNames) {
                if (objName.getType() === SqlMonikerType.FUNCTION) {
                    result.add(objName)
                }
            }
        }

        private fun findAllValidFunctionNames(
            names: List<String>,
            validator: SqlValidator,
            result: Collection<SqlMoniker>,
            pos: SqlParserPos
        ) {
            // a function name can only be 1 part
            if (names.size() > 1) {
                return
            }
            for (op in validator.getOperatorTable().getOperatorList()) {
                val curOpId = SqlIdentifier(
                    op.getName(),
                    pos
                )
                val call: SqlCall = validator.makeNullaryCall(curOpId)
                if (call != null) {
                    result.add(
                        SqlMonikerImpl(
                            op.getName(),
                            SqlMonikerType.FUNCTION
                        )
                    )
                } else {
                    if (op.getSyntax() === SqlSyntax.FUNCTION
                        || op.getSyntax() === SqlSyntax.PREFIX
                    ) {
                        if (op.getOperandTypeChecker() != null) {
                            var sig: String = op.getAllowedSignatures()
                            sig = sig.replace("'", "")
                            result.add(
                                SqlMonikerImpl(
                                    sig,
                                    SqlMonikerType.FUNCTION
                                )
                            )
                            continue
                        }
                        result.add(
                            SqlMonikerImpl(
                                op.getName(),
                                SqlMonikerType.FUNCTION
                            )
                        )
                    }
                }
            }
        }

        @Nullable
        private fun getInnerSelect(node: SqlNode?): SqlSelect? {
            var node: SqlNode? = node
            while (true) {
                node = if (node is SqlSelect) {
                    return node as SqlSelect?
                } else if (node is SqlOrderBy) {
                    (node as SqlOrderBy?).query
                } else if (node is SqlWith) {
                    (node as SqlWith?).body
                } else {
                    return null
                }
            }
        }

        private fun rewriteMerge(call: SqlMerge?) {
            var selectList: SqlNodeList
            val updateStmt: SqlUpdate = call.getUpdateCall()
            if (updateStmt != null) {
                // if we have an update statement, just clone the select list
                // from the update statement's source since it's the same as
                // what we want for the select list of the merge source -- '*'
                // followed by the update set expressions
                val sourceSelect: SqlSelect = SqlNonNullableAccessors.getSourceSelect(updateStmt)
                selectList = SqlNode.clone(SqlNonNullableAccessors.getSelectList(sourceSelect))
            } else {
                // otherwise, just use select *
                selectList = SqlNodeList(SqlParserPos.ZERO)
                selectList.add(SqlIdentifier.star(SqlParserPos.ZERO))
            }
            var targetTable: SqlNode = call.getTargetTable()
            if (call.getAlias() != null) {
                targetTable = SqlValidatorUtil.addAlias(
                    targetTable,
                    call.getAlias().getSimple()
                )
            }

            // Provided there is an insert substatement, the source select for
            // the merge is a left outer join between the source in the USING
            // clause and the target table; otherwise, the join is just an
            // inner join.  Need to clone the source table reference in order
            // for validation to work
            val sourceTableRef: SqlNode = call.getSourceTableRef()
            val insertCall: SqlInsert = call.getInsertCall()
            val joinType: JoinType = if (insertCall == null) JoinType.INNER else JoinType.LEFT
            val leftJoinTerm: SqlNode = SqlNode.clone(sourceTableRef)
            val outerJoin: SqlNode = SqlJoin(
                SqlParserPos.ZERO,
                leftJoinTerm,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                joinType.symbol(SqlParserPos.ZERO),
                targetTable,
                JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                call.getCondition()
            )
            var select: SqlSelect? = SqlSelect(
                SqlParserPos.ZERO, null, selectList, outerJoin, null,
                null, null, null, null, null, null, null
            )
            call.setSourceSelect(select)

            // Source for the insert call is a select of the source table
            // reference with the select list being the value expressions;
            // note that the values clause has already been converted to a
            // select on the values row constructor; so we need to extract
            // that via the from clause on the select
            if (insertCall != null) {
                val valuesCall: SqlCall = insertCall.getSource() as SqlCall
                val rowCall: SqlCall = valuesCall.operand(0)
                selectList = SqlNodeList(
                    rowCall.getOperandList(),
                    SqlParserPos.ZERO
                )
                val insertSource: SqlNode = SqlNode.clone(sourceTableRef)
                select = SqlSelect(
                    SqlParserPos.ZERO, null, selectList, insertSource, null,
                    null, null, null, null, null, null, null
                )
                insertCall.setSource(select)
            }
        }

        @Nullable
        private fun stripDot(@Nullable node: SqlNode?): SqlNode? {
            return if (node != null && node.getKind() === SqlKind.DOT) {
                stripDot((node as SqlCall).operand(0))
            } else node
        }

        @Nullable
        private fun getWindowInOver(over: SqlNode?): SqlWindow? {
            if (over.getKind() === SqlKind.OVER) {
                val window: SqlNode = (over as SqlCall?).getOperandList().get(1)
                return if (window is SqlWindow) {
                    window as SqlWindow
                } else null
                // SqlIdentifier, gets validated elsewhere
            }
            return null
        }

        private fun stripOver(node: SqlNode?): SqlNode? {
            return when (node.getKind()) {
                OVER -> (node as SqlCall?).getOperandList().get(0)
                else -> node
            }
        }

        @Nullable
        private fun resolveTable(identifier: SqlIdentifier, scope: SqlValidatorScope): Table? {
            val fullyQualified: SqlQualified = scope.fullyQualify(identifier)
            assert(fullyQualified.namespace != null) { "namespace must not be null in $fullyQualified" }
            val sqlValidatorTable: SqlValidatorTable = fullyQualified.namespace.getTable()
            return if (sqlValidatorTable != null) {
                sqlValidatorTable.table()
            } else null
        }

        private fun shouldCheckForRollUp(@Nullable from: SqlNode?): Boolean {
            if (from != null) {
                val kind: SqlKind = stripAs(from).getKind()
                return kind !== SqlKind.VALUES && kind !== SqlKind.SELECT
            }
            return false
        }

        /** Return the intended modality of a SELECT or set-op.  */
        private fun deduceModality(query: SqlNode): SqlModality {
            return if (query is SqlSelect) {
                val select: SqlSelect = query as SqlSelect
                if (select.getModifierNode(SqlSelectKeyword.STREAM) != null) SqlModality.STREAM else SqlModality.RELATION
            } else if (query.getKind() === SqlKind.VALUES) {
                SqlModality.RELATION
            } else {
                assert(query.isA(SqlKind.SET_QUERY))
                val call: SqlCall = query as SqlCall
                deduceModality(call.getOperandList().get(0))
            }
        }

        /** Returns whether the prefix is sorted.  */
        private fun hasSortedPrefix(scope: SelectScope, orderList: SqlNodeList): Boolean {
            return isSortCompatible(scope, orderList.get(0), false)
        }

        private fun isSortCompatible(
            scope: SelectScope, node: SqlNode,
            descending: Boolean
        ): Boolean {
            when (node.getKind()) {
                DESCENDING -> return isSortCompatible(
                    scope, (node as SqlCall).getOperandList().get(0),
                    true
                )
                else -> {}
            }
            val monotonicity: SqlMonotonicity = scope.getMonotonicity(node)
            return when (monotonicity) {
                INCREASING, STRICTLY_INCREASING -> !descending
                DECREASING, STRICTLY_DECREASING -> descending
                else -> false
            }
        }

        /** Returns whether a query uses `DEFAULT` to populate a given
         * column.  */
        private fun isValuesWithDefault(source: SqlNode, column: Int): Boolean {
            when (source.getKind()) {
                VALUES -> {
                    for (operand in (source as SqlCall).getOperandList()) {
                        if (!isRowWithDefault(operand, column)) {
                            return false
                        }
                    }
                    return true
                }
                else -> {}
            }
            return false
        }

        private fun isRowWithDefault(operand: SqlNode, column: Int): Boolean {
            when (operand.getKind()) {
                ROW -> {
                    val row: SqlCall = operand as SqlCall
                    return (row.getOperandList().size() >= column
                            && row.getOperandList().get(column).getKind() === SqlKind.DEFAULT)
                }
                else -> {}
            }
            return false
        }

        /**
         * Locates the n'th expression in an INSERT or UPDATE query.
         *
         * @param query       Query
         * @param ordinal     Ordinal of expression
         * @param sourceCount Number of expressions
         * @return Ordinal'th expression, never null
         */
        private fun getNthExpr(query: SqlNode, ordinal: Int, sourceCount: Int): SqlNode {
            return if (query is SqlInsert) {
                val insert: SqlInsert = query as SqlInsert
                if (insert.getTargetColumnList() != null) {
                    insert.getTargetColumnList().get(ordinal)
                } else {
                    getNthExpr(
                        insert.getSource(),
                        ordinal,
                        sourceCount
                    )
                }
            } else if (query is SqlUpdate) {
                val update: SqlUpdate = query as SqlUpdate
                if (update.getSourceExpressionList() != null) {
                    update.getSourceExpressionList().get(ordinal)
                } else {
                    getNthExpr(
                        SqlNonNullableAccessors.getSourceSelect(update),
                        ordinal,
                        sourceCount
                    )
                }
            } else if (query is SqlSelect) {
                val select: SqlSelect = query as SqlSelect
                val selectList: SqlNodeList = SqlNonNullableAccessors.getSelectList(select)
                if (selectList.size() === sourceCount) {
                    selectList.get(ordinal)
                } else {
                    query // give up
                }
            } else {
                query // give up
            }
        }

        /** Returns the alias of a "expr AS alias" expression.  */
        private fun alias(item: SqlNode): String {
            assert(item is SqlCall)
            assert(item.getKind() === SqlKind.AS)
            val identifier: SqlIdentifier = (item as SqlCall).operand(1)
            return identifier.getSimple()
        }

        private fun isPhysicalNavigation(kind: SqlKind): Boolean {
            return kind === SqlKind.PREV || kind === SqlKind.NEXT
        }

        private fun isLogicalNavigation(kind: SqlKind): Boolean {
            return kind === SqlKind.FIRST || kind === SqlKind.LAST
        }

        private fun isAggregation(kind: SqlKind): Boolean {
            return kind === SqlKind.SUM || kind === SqlKind.SUM0 || kind === SqlKind.AVG || kind === SqlKind.COUNT || kind === SqlKind.MAX || kind === SqlKind.MIN
        }

        private fun isRunningOrFinal(kind: SqlKind): Boolean {
            return kind === SqlKind.RUNNING || kind === SqlKind.FINAL
        }

        private fun isSingleVarRequired(kind: SqlKind): Boolean {
            return (isPhysicalNavigation(kind)
                    || isLogicalNavigation(kind)
                    || isAggregation(kind))
        }
    }
}
