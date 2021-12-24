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

import org.apache.calcite.config.CalciteConnectionConfig

/**
 * Utility methods related to validation.
 */
object SqlValidatorUtil {
    //~ Methods ----------------------------------------------------------------
    /**
     * Converts a [SqlValidatorScope] into a [RelOptTable]. This is
     * only possible if the scope represents an identifier, such as "sales.emp".
     * Otherwise, returns null.
     *
     * @param namespace     Namespace
     * @param catalogReader Schema
     * @param datasetName   Name of sample dataset to substitute, or null to use
     * the regular table
     * @param usedDataset   Output parameter which is set to true if a sample
     * dataset is found; may be null
     */
    @Nullable
    fun getRelOptTable(
        namespace: SqlValidatorNamespace,
        catalogReader: @Nullable CatalogReader?,
        @Nullable datasetName: String?,
        usedDataset: @Nullable BooleanArray?
    ): RelOptTable? {
        if (namespace.isWrapperFor(TableNamespace::class.java)) {
            val tableNamespace: TableNamespace = namespace.unwrap(TableNamespace::class.java)
            return getRelOptTable(
                tableNamespace,
                requireNonNull(catalogReader, "catalogReader"), datasetName, usedDataset,
                tableNamespace.extendedFields
            )
        } else if (namespace.isWrapperFor(SqlValidatorImpl.DmlNamespace::class.java)) {
            val dmlNamespace: SqlValidatorImpl.DmlNamespace = namespace.unwrap(
                SqlValidatorImpl.DmlNamespace::class.java
            )
            val resolvedNamespace: SqlValidatorNamespace = dmlNamespace.resolve()
            if (resolvedNamespace.isWrapperFor(TableNamespace::class.java)) {
                val tableNamespace: TableNamespace = resolvedNamespace.unwrap(TableNamespace::class.java)
                val validatorTable: SqlValidatorTable = tableNamespace.getTable()
                val extendedFields: List<RelDataTypeField> =
                    if (dmlNamespace.extendList == null) ImmutableList.of() else getExtendedColumns(
                        namespace.getValidator(),
                        validatorTable,
                        dmlNamespace.extendList
                    )
                return getRelOptTable(
                    tableNamespace, requireNonNull(catalogReader, "catalogReader"),
                    datasetName, usedDataset, extendedFields
                )
            }
        }
        return null
    }

    @Nullable
    private fun getRelOptTable(
        tableNamespace: TableNamespace,
        catalogReader: CatalogReader,
        @Nullable datasetName: String?,
        usedDataset: @Nullable BooleanArray?,
        extendedFields: List<RelDataTypeField>
    ): RelOptTable? {
        val names: List<String> = tableNamespace.getTable().getQualifiedName()
        var table: RelOptTable
        table = if (datasetName != null
            && catalogReader is RelOptSchemaWithSampling
        ) {
            val reader: RelOptSchemaWithSampling = catalogReader as RelOptSchemaWithSampling
            reader.getTableForMember(names, datasetName, usedDataset)
        } else {
            // Schema does not support substitution. Ignore the data set, if any.
            catalogReader.getTableForMember(names)
        }
        if (table != null && !extendedFields.isEmpty()) {
            table = table.extend(extendedFields)
        }
        return table
    }

    /**
     * Gets a list of extended columns with field indices to the underlying table.
     */
    fun getExtendedColumns(
        @Nullable validator: SqlValidator?, table: SqlValidatorTable, extendedColumns: SqlNodeList
    ): List<RelDataTypeField> {
        val extendedFields: ImmutableList.Builder<RelDataTypeField> = ImmutableList.builder()
        val extTable: ExtensibleTable = table.unwrap(ExtensibleTable::class.java)
        var extendedFieldOffset: Int =
            if (extTable == null) table.getRowType().getFieldCount() else extTable.getExtendedColumnOffset()
        for (pair in pairs(extendedColumns)) {
            val identifier: SqlIdentifier = pair.left
            val type: SqlDataTypeSpec = pair.right
            extendedFields.add(
                RelDataTypeFieldImpl(
                    identifier.toString(),
                    extendedFieldOffset++,
                    type.deriveType(requireNonNull(validator, "validator"))
                )
            )
        }
        return extendedFields.build()
    }

    /** Converts a list of extended columns
     * (of the form [name0, type0, name1, type1, ...])
     * into a list of (name, type) pairs.  */
    @SuppressWarnings(["unchecked", "rawtypes"])
    private fun pairs(
        extendedColumns: SqlNodeList
    ): List<Pair<SqlIdentifier, SqlDataTypeSpec>> {
        return Util.pairs(extendedColumns as List)
    }

    /**
     * Gets a map of indexes from the source to fields in the target for the
     * intersecting set of source and target fields.
     *
     * @param sourceFields The source of column names that determine indexes
     * @param targetFields The target fields to be indexed
     */
    fun getIndexToFieldMap(
        sourceFields: List<RelDataTypeField?>,
        targetFields: RelDataType
    ): ImmutableMap<Integer, RelDataTypeField> {
        val output: ImmutableMap.Builder<Integer, RelDataTypeField> = ImmutableMap.builder()
        for (source in sourceFields) {
            val target: RelDataTypeField = targetFields.getField(source.getName(), true, false)
            if (target != null) {
                output.put(source.getIndex(), target)
            }
        }
        return output.build()
    }

    /**
     * Gets the bit-set to the column ordinals in the source for columns that intersect in the target.
     * @param sourceRowType The source upon which to ordinate the bit set.
     * @param targetRowType The target to overlay on the source to create the bit set.
     */
    fun getOrdinalBitSet(
        sourceRowType: RelDataType, targetRowType: RelDataType
    ): ImmutableBitSet {
        val indexToField: Map<Integer, RelDataTypeField> =
            getIndexToFieldMap(sourceRowType.getFieldList(), targetRowType)
        return getOrdinalBitSet(sourceRowType, indexToField)
    }

    /**
     * Gets the bit-set to the column ordinals in the source for columns that
     * intersect in the target.
     *
     * @param sourceRowType The source upon which to ordinate the bit set.
     * @param indexToField  The map of ordinals to target fields.
     */
    fun getOrdinalBitSet(
        sourceRowType: RelDataType,
        indexToField: Map<Integer?, RelDataTypeField?>
    ): ImmutableBitSet {
        val source: ImmutableBitSet = ImmutableBitSet.of(
            Util.transform(sourceRowType.getFieldList(), RelDataTypeField::getIndex)
        )
        // checkerframework: found   : Set<@KeyFor("indexToField") Integer>
        val target: ImmutableBitSet = ImmutableBitSet.of(indexToField.keySet() as Iterable<Integer?>?)
        return source.intersect(target)
    }

    /** Returns a map from field names to indexes.  */
    fun mapNameToIndex(fields: List<RelDataTypeField?>): Map<String, Integer> {
        val output: ImmutableMap.Builder<String, Integer> = ImmutableMap.builder()
        for (field in fields) {
            output.put(field.getName(), field.getIndex())
        }
        return output.build()
    }

    @Deprecated // to be removed before 2.0
    @Nullable
    fun lookupField(
        caseSensitive: Boolean,
        rowType: RelDataType, columnName: String?
    ): RelDataTypeField {
        return rowType.getField(columnName, caseSensitive, false)
    }

    fun checkCharsetAndCollateConsistentIfCharType(
        type: RelDataType
    ) {
        // (every charset must have a default collation)
        if (SqlTypeUtil.inCharFamily(type)) {
            val strCharset: Charset = getCharset(type)
            val colCharset: Charset = getCollation(type).getCharset()
            if (!strCharset.equals(colCharset)) {
                if (false) {
                    // todo: enable this checking when we have a charset to
                    //   collation mapping
                    throw Error(
                        type.toString()
                                + " was found to have charset '" + strCharset.name()
                                + "' and a mismatched collation charset '"
                                + colCharset.name() + "'"
                    )
                }
            }
        }
    }

    /**
     * Checks that there are no duplicates in a list of [SqlIdentifier].
     */
    fun checkIdentifierListForDuplicates(
        columnList: List<SqlNode?>,
        validationErrorFunction: SqlValidatorImpl.ValidationErrorFunction
    ) {
        val names: List<List<String>> = Util.transform(
            columnList
        ) { sqlNode -> (requireNonNull(sqlNode, "sqlNode") as SqlIdentifier).names }
        val i: Int = Util.firstDuplicate(names)
        if (i >= 0) {
            throw validationErrorFunction.apply(
                requireNonNull(columnList[i]) { "$columnList.get($i)" },
                RESOURCE.duplicateNameInColumnList(Util.last(names[i]))
            )
        }
    }

    /**
     * Converts an expression "expr" into "expr AS alias".
     */
    fun addAlias(
        expr: SqlNode,
        alias: String?
    ): SqlNode {
        val pos: SqlParserPos = expr.getParserPosition()
        val id = SqlIdentifier(alias, pos)
        return SqlStdOperatorTable.AS.createCall(pos, expr, id)
    }

    /**
     * Derives an alias for a node, and invents a mangled identifier if it
     * cannot.
     *
     *
     * Examples:
     *
     *
     *  * Alias: "1 + 2 as foo" yields "foo"
     *  * Identifier: "foo.bar.baz" yields "baz"
     *  * Anything else yields "expr$*ordinal*"
     *
     *
     * @return An alias, if one can be derived; or a synthetic alias
     * "expr$*ordinal*" if ordinal &lt; 0; otherwise null
     */
    @Nullable
    fun getAlias(node: SqlNode?, ordinal: Int): String? {
        return when (node.getKind()) {
            AS ->       // E.g. "1 + 2 as foo" --> "foo"
                (node as SqlCall?).operand(1).toString()
            OVER ->       // E.g. "bids over w" --> "bids"
                getAlias((node as SqlCall?).operand(0), ordinal)
            IDENTIFIER ->       // E.g. "foo.bar" --> "bar"
                Util.last((node as SqlIdentifier?).names)
            else -> if (ordinal < 0) {
                null
            } else {
                SqlUtil.deriveAliasFromOrdinal(ordinal)
            }
        }
    }

    /**
     * Factory method for [SqlValidator].
     */
    fun newValidator(
        opTab: SqlOperatorTable?,
        catalogReader: SqlValidatorCatalogReader,
        typeFactory: RelDataTypeFactory,
        config: SqlValidator.Config
    ): SqlValidatorWithHints {
        return SqlValidatorImpl(
            opTab, catalogReader, typeFactory,
            config
        )
    }

    /**
     * Factory method for [SqlValidator], with default conformance.
     */
    @Deprecated // to be removed before 2.0
    fun newValidator(
        opTab: SqlOperatorTable?,
        catalogReader: SqlValidatorCatalogReader,
        typeFactory: RelDataTypeFactory
    ): SqlValidatorWithHints {
        return newValidator(
            opTab, catalogReader, typeFactory,
            SqlValidator.Config.DEFAULT
        )
    }

    /**
     * Makes a name distinct from other names which have already been used, adds
     * it to the list, and returns it.
     *
     * @param name      Suggested name, may not be unique
     * @param usedNames  Collection of names already used
     * @param suggester Base for name when input name is null
     * @return Unique name
     */
    fun uniquify(
        @Nullable name: String?, usedNames: Set<String?>,
        suggester: Suggester
    ): String? {
        var name = name
        if (name != null) {
            if (usedNames.add(name)) {
                return name
            }
        }
        val originalName = name
        var j = 0
        while (true) {
            name = suggester.apply(originalName, j, usedNames.size())
            if (usedNames.add(name)) {
                return name
            }
            j++
        }
    }

    /**
     * Makes sure that the names in a list are unique.
     *
     *
     * Does not modify the input list. Returns the input list if the strings
     * are unique, otherwise allocates a new list. Deprecated in favor of caseSensitive
     * aware version.
     *
     * @param nameList List of strings
     * @return List of unique strings
     */
    @Deprecated // to be removed before 2.0
    fun uniquify(nameList: List<String?>): List<String?> {
        return uniquify(nameList, EXPR_SUGGESTER, true)
    }

    /**
     * Makes sure that the names in a list are unique.
     *
     *
     * Does not modify the input list. Returns the input list if the strings
     * are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param suggester How to generate new names if duplicate names are found
     * @return List of unique strings
     */
    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Use {@link #uniquify(List, Suggester, boolean)}

    """
    )
    fun uniquify(nameList: List<String?>, suggester: Suggester): List<String?> {
        return uniquify(nameList, suggester, true)
    }

    /**
     * Makes sure that the names in a list are unique.
     *
     *
     * Does not modify the input list. Returns the input list if the strings
     * are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param caseSensitive Whether upper and lower case names are considered
     * distinct
     * @return List of unique strings
     */
    fun uniquify(
        nameList: List<String?>,
        caseSensitive: Boolean
    ): List<String?> {
        return uniquify(nameList, EXPR_SUGGESTER, caseSensitive)
    }

    /**
     * Makes sure that the names in a list are unique.
     *
     *
     * Does not modify the input list. Returns the input list if the strings
     * are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param suggester How to generate new names if duplicate names are found
     * @param caseSensitive Whether upper and lower case names are considered
     * distinct
     * @return List of unique strings
     */
    fun uniquify(
        nameList: List<String?>,
        suggester: Suggester,
        caseSensitive: Boolean
    ): List<String?> {
        val used: Set<String> = if (caseSensitive) LinkedHashSet() else TreeSet(String.CASE_INSENSITIVE_ORDER)
        var changeCount = 0
        val newNameList: List<String> = ArrayList()
        for (name in nameList) {
            val uniqueName = uniquify(name, used, suggester)
            if (!uniqueName!!.equals(name)) {
                ++changeCount
            }
            newNameList.add(uniqueName)
        }
        return if (changeCount == 0) nameList else newNameList
    }

    /**
     * Derives the type of a join relational expression.
     *
     * @param leftType        Row type of left input to join
     * @param rightType       Row type of right input to join
     * @param joinType        Type of join
     * @param typeFactory     Type factory
     * @param fieldNameList   List of names of fields; if null, field names are
     * inherited and made unique
     * @param systemFieldList List of system fields that will be prefixed to
     * output row type; typically empty but must not be
     * null
     * @return join type
     */
    fun deriveJoinRowType(
        leftType: RelDataType,
        @Nullable rightType: RelDataType?,
        joinType: JoinRelType?,
        typeFactory: RelDataTypeFactory,
        @Nullable fieldNameList: List<String?>?,
        systemFieldList: List<RelDataTypeField?>?
    ): RelDataType {
        var leftType: RelDataType = leftType
        var rightType: RelDataType? = rightType
        assert(systemFieldList != null)
        when (joinType) {
            LEFT -> rightType = typeFactory.createTypeWithNullability(
                requireNonNull(rightType, "rightType"), true
            )
            RIGHT -> leftType = typeFactory.createTypeWithNullability(leftType, true)
            FULL -> {
                leftType = typeFactory.createTypeWithNullability(leftType, true)
                rightType = typeFactory.createTypeWithNullability(
                    requireNonNull(rightType, "rightType"), true
                )
            }
            SEMI, ANTI -> rightType = null
            else -> {}
        }
        return createJoinType(
            typeFactory, leftType, rightType, fieldNameList,
            systemFieldList
        )
    }

    /**
     * Returns the type the row which results when two relations are joined.
     *
     *
     * The resulting row type consists of
     * the system fields (if any), followed by
     * the fields of the left type, followed by
     * the fields of the right type. The field name list, if present, overrides
     * the original names of the fields.
     *
     * @param typeFactory     Type factory
     * @param leftType        Type of left input to join
     * @param rightType       Type of right input to join, or null for semi-join
     * @param fieldNameList   If not null, overrides the original names of the
     * fields
     * @param systemFieldList List of system fields that will be prefixed to
     * output row type; typically empty but must not be
     * null
     * @return type of row which results when two relations are joined
     */
    fun createJoinType(
        typeFactory: RelDataTypeFactory,
        leftType: RelDataType,
        @Nullable rightType: RelDataType?,
        @Nullable fieldNameList: List<String?>?,
        systemFieldList: List<RelDataTypeField?>?
    ): RelDataType {
        assert(
            fieldNameList == null
                    || (fieldNameList.size()
                    === (systemFieldList.size()
                    + leftType.getFieldCount()
                    + if (rightType == null) 0 else rightType.getFieldCount()))
        )
        var nameList: List<String?> = ArrayList()
        val typeList: List<RelDataType> = ArrayList()

        // Use a set to keep track of the field names; this is needed
        // to ensure that the contains() call to check for name uniqueness
        // runs in constant time; otherwise, if the number of fields is large,
        // doing a contains() on a list can be expensive.
        val uniqueNameList: Set<String> = if (typeFactory.getTypeSystem()
                .isSchemaCaseSensitive()
        ) HashSet() else TreeSet(String.CASE_INSENSITIVE_ORDER)
        addFields(systemFieldList, typeList, nameList, uniqueNameList)
        addFields(leftType.getFieldList(), typeList, nameList, uniqueNameList)
        if (rightType != null) {
            addFields(
                rightType.getFieldList(), typeList, nameList, uniqueNameList
            )
        }
        if (fieldNameList != null) {
            assert(fieldNameList.size() === nameList.size())
            nameList = fieldNameList
        }
        return typeFactory.createStructType(typeList, nameList)
    }

    private fun addFields(
        fieldList: List<RelDataTypeField?>?,
        typeList: List<RelDataType>, nameList: List<String?>,
        uniqueNames: Set<String>
    ) {
        for (field in fieldList) {
            var name: String = field.getName()

            // Ensure that name is unique from all previous field names
            if (uniqueNames.contains(name)) {
                val nameBase = name
                var j = 0
                while (true) {
                    name = nameBase + j
                    if (!uniqueNames.contains(name)) {
                        break
                    }
                    j++
                }
            }
            nameList.add(name)
            uniqueNames.add(name)
            typeList.add(field.getType())
        }
    }

    /**
     * Resolve a target column name in the target table.
     *
     * @return the target field or null if the name cannot be resolved
     * @param rowType the target row type
     * @param id      the target column identifier
     * @param table   the target table or null if it is not a RelOptTable instance
     */
    @Nullable
    fun getTargetField(
        rowType: RelDataType?, typeFactory: RelDataTypeFactory?,
        id: SqlIdentifier, catalogReader: SqlValidatorCatalogReader,
        @Nullable table: RelOptTable?
    ): RelDataTypeField? {
        val t: Table? = if (table == null) null else table.unwrap(Table::class.java)
        if (t !is CustomColumnResolvingTable) {
            val nameMatcher: SqlNameMatcher = catalogReader.nameMatcher()
            return nameMatcher.field(rowType, id.getSimple())
        }
        val entries: List<Pair<RelDataTypeField, List<String>>> = (t as CustomColumnResolvingTable?).resolveColumn(
            rowType, typeFactory, id.names
        )
        return when (entries.size()) {
            1 -> {
                if (!entries[0].getValue().isEmpty()) {
                    null
                } else entries[0].getKey()
            }
            else -> null
        }
    }

    /**
     * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a
     * namespace. The returned namespace, never null, may represent a
     * schema, table, column, etc.
     */
    fun lookup(
        scope: SqlValidatorScope,
        names: List<String?>
    ): SqlValidatorNamespace? {
        assert(names.size() > 0)
        val nameMatcher: SqlNameMatcher = scope.getValidator().getCatalogReader().nameMatcher()
        val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
        scope.resolve(ImmutableList.of(names[0]), nameMatcher, false, resolved)
        assert(resolved.count() === 1)
        var namespace: SqlValidatorNamespace = resolved.only().namespace
        for (name in Util.skip(names)) {
            namespace = namespace!!.lookupChild(name)
            assert(namespace != null)
        }
        return namespace
    }

    fun getSchemaObjectMonikers(
        catalogReader: SqlValidatorCatalogReader,
        names: List<String?>?,
        hints: List<SqlMoniker?>
    ) {
        // Assume that the last name is 'dummy' or similar.
        val subNames: List<String> = Util.skipLast(names)

        // Try successively with catalog.schema, catalog and no prefix
        for (x in catalogReader.getSchemaPaths()) {
            val names2: List<String> =
                ImmutableList.< String > builder < String ? > ().addAll(x).addAll(subNames).build()
            hints.addAll(catalogReader.getAllSchemaObjectNames(names2))
        }
    }

    @Nullable
    fun getEnclosingSelectScope(@Nullable scope: SqlValidatorScope?): SelectScope? {
        var scope: SqlValidatorScope? = scope
        while (scope is DelegatingScope) {
            if (scope is SelectScope) {
                return scope as SelectScope?
            }
            scope = (scope as DelegatingScope?).getParent()
        }
        return null
    }

    @Nullable
    fun getEnclosingAggregateSelectScope(
        scope: SqlValidatorScope
    ): AggregatingSelectScope? {
        var scope: SqlValidatorScope = scope
        while (scope is DelegatingScope) {
            if (scope is AggregatingSelectScope) {
                return scope as AggregatingSelectScope
            }
            scope = (scope as DelegatingScope).getParent()
        }
        return null
    }

    /**
     * Derives the list of column names suitable for NATURAL JOIN. These are the
     * columns that occur exactly once on each side of the join.
     *
     * @param nameMatcher Whether matches are case-sensitive
     * @param leftRowType  Row type of left input to the join
     * @param rightRowType Row type of right input to the join
     * @return List of columns that occur once on each side
     */
    fun deriveNaturalJoinColumnList(
        nameMatcher: SqlNameMatcher,
        leftRowType: RelDataType,
        rightRowType: RelDataType
    ): List<String> {
        val naturalColumnNames: List<String> = ArrayList()
        val leftNames: List<String> = leftRowType.getFieldNames()
        val rightNames: List<String> = rightRowType.getFieldNames()
        for (name in leftNames) {
            if (nameMatcher.frequency(leftNames, name) === 1
                && nameMatcher.frequency(rightNames, name) === 1
            ) {
                naturalColumnNames.add(name)
            }
        }
        return naturalColumnNames
    }

    fun createTypeFromProjection(
        type: RelDataType,
        columnNameList: List<String>, typeFactory: RelDataTypeFactory,
        caseSensitive: Boolean
    ): RelDataType {
        // If the names in columnNameList and type have case-sensitive differences,
        // the resulting type will use those from type. These are presumably more
        // canonical.
        val fields: List<RelDataTypeField> = ArrayList(columnNameList.size())
        for (name in columnNameList) {
            val field: RelDataTypeField = type.getField(name, caseSensitive, false)
            assert(field != null) {
                ("field " + name + (if (caseSensitive) " (caseSensitive)" else "")
                        + " is not found in " + type)
            }
            fields.add(type.getFieldList().get(field.getIndex()))
        }
        return typeFactory.createStructType(fields)
    }

    /** Analyzes an expression in a GROUP BY clause.
     *
     *
     * It may be an expression, an empty list (), or a call to
     * `GROUPING SETS`, `CUBE`, `ROLLUP`,
     * `TUMBLE`, `HOP` or `SESSION`.
     *
     *
     * Each group item produces a list of group sets, which are written to
     * `topBuilder`. To find the grouping sets of the query, we will take
     * the cartesian product of the group sets.  */
    fun analyzeGroupItem(
        scope: SqlValidatorScope,
        groupAnalyzer: GroupAnalyzer,
        topBuilder: ImmutableList.Builder<ImmutableList<ImmutableBitSet?>?>,
        groupExpr: SqlNode
    ) {
        val builder: ImmutableList.Builder<ImmutableBitSet>
        when (groupExpr.getKind()) {
            CUBE, ROLLUP -> {
                // E.g. ROLLUP(a, (b, c)) becomes [{0}, {1, 2}]
                // then we roll up to [(0, 1, 2), (0), ()]  -- note no (0, 1)
                val bitSets: List<ImmutableBitSet> = analyzeGroupTuple(
                    scope, groupAnalyzer,
                    (groupExpr as SqlCall).getOperandList()
                )
                when (groupExpr.getKind()) {
                    ROLLUP -> {
                        topBuilder.add(rollup(bitSets))
                        return
                    }
                    else -> {
                        topBuilder.add(cube(bitSets))
                        return
                    }
                }
            }
            OTHER -> {
                if (groupExpr is SqlNodeList) {
                    val list: SqlNodeList = groupExpr as SqlNodeList
                    for (node in list) {
                        analyzeGroupItem(
                            scope, groupAnalyzer, topBuilder,
                            node
                        )
                    }
                    return
                }
                builder = ImmutableList.builder()
                convertGroupSet(
                    scope, groupAnalyzer, builder,
                    groupExpr
                )
                topBuilder.add(builder.build())
            }
            HOP, TUMBLE, SESSION, GROUPING_SETS -> {
                builder = ImmutableList.builder()
                convertGroupSet(
                    scope, groupAnalyzer, builder,
                    groupExpr
                )
                topBuilder.add(builder.build())
            }
            else -> {
                builder = ImmutableList.builder()
                convertGroupSet(
                    scope, groupAnalyzer, builder,
                    groupExpr
                )
                topBuilder.add(builder.build())
            }
        }
    }

    /** Analyzes a GROUPING SETS item in a GROUP BY clause.  */
    private fun convertGroupSet(
        scope: SqlValidatorScope,
        groupAnalyzer: GroupAnalyzer,
        builder: ImmutableList.Builder<ImmutableBitSet>, groupExpr: SqlNode
    ) {
        when (groupExpr.getKind()) {
            GROUPING_SETS -> {
                val call: SqlCall = groupExpr as SqlCall
                for (node in call.getOperandList()) {
                    convertGroupSet(scope, groupAnalyzer, builder, node)
                }
                return
            }
            ROW -> {
                val bitSets: List<ImmutableBitSet> = analyzeGroupTuple(
                    scope, groupAnalyzer,
                    (groupExpr as SqlCall).getOperandList()
                )
                builder.add(ImmutableBitSet.union(bitSets))
                return
            }
            ROLLUP, CUBE -> {

                // GROUPING SETS ( (a), ROLLUP(c,b), CUBE(d,e) )
                // is EQUIVALENT to
                // GROUPING SETS ( (a), (c,b), (b) ,(), (d,e), (d), (e) ).
                // Expand all ROLLUP/CUBE nodes
                val operandBitSet: List<ImmutableBitSet> = analyzeGroupTuple(
                    scope, groupAnalyzer,
                    (groupExpr as SqlCall).getOperandList()
                )
                when (groupExpr.getKind()) {
                    ROLLUP -> {
                        builder.addAll(rollup(operandBitSet))
                        return
                    }
                    else -> {
                        builder.addAll(cube(operandBitSet))
                        return
                    }
                }
            }
            else -> {
                builder.add(
                    analyzeGroupExpr(scope, groupAnalyzer, groupExpr)
                )
                return
            }
        }
    }

    /** Analyzes a tuple in a GROUPING SETS clause.
     *
     *
     * For example, in `GROUP BY GROUPING SETS ((a, b), a, c)`,
     * `(a, b)` is a tuple.
     *
     *
     * Gathers into `groupExprs` the set of distinct expressions being
     * grouped, and returns a bitmap indicating which expressions this tuple
     * is grouping.  */
    private fun analyzeGroupTuple(
        scope: SqlValidatorScope,
        groupAnalyzer: GroupAnalyzer, operandList: List<SqlNode>
    ): List<ImmutableBitSet> {
        val list: List<ImmutableBitSet> = ArrayList()
        for (operand in operandList) {
            list.add(
                analyzeGroupExpr(scope, groupAnalyzer, operand)
            )
        }
        return list
    }

    /** Analyzes a component of a tuple in a GROUPING SETS clause.  */
    private fun analyzeGroupExpr(
        scope: SqlValidatorScope,
        groupAnalyzer: GroupAnalyzer,
        groupExpr: SqlNode
    ): ImmutableBitSet {
        val expandedGroupExpr: SqlNode = scope.getValidator().expand(groupExpr, scope)
        when (expandedGroupExpr.getKind()) {
            ROW -> return ImmutableBitSet.union(
                analyzeGroupTuple(
                    scope, groupAnalyzer,
                    (expandedGroupExpr as SqlCall).getOperandList()
                )
            )
            OTHER -> if (expandedGroupExpr is SqlNodeList
                && (expandedGroupExpr as SqlNodeList).size() === 0
            ) {
                return ImmutableBitSet.of()
            }
            else -> {}
        }
        val ref = lookupGroupExpr(groupAnalyzer, expandedGroupExpr)
        if (expandedGroupExpr is SqlIdentifier) {
            // SQL 2003 does not allow expressions of column references
            val expr: SqlIdentifier = expandedGroupExpr as SqlIdentifier
            assert(expr.names.size() >= 2)
            val originalRelName: String = expr.names.get(0)
            val originalFieldName: String = expr.names.get(1)
            val nameMatcher: SqlNameMatcher = scope.getValidator().getCatalogReader().nameMatcher()
            val resolved: SqlValidatorScope.ResolvedImpl = ResolvedImpl()
            scope.resolve(
                ImmutableList.of(originalRelName), nameMatcher, false,
                resolved
            )
            assert(resolved.count() === 1)
            val resolve: Resolve = resolved.only()
            val rowType: RelDataType = resolve.rowType()
            val childNamespaceIndex: Int = resolve.path.steps().get(0).i
            var namespaceOffset = 0
            if (childNamespaceIndex > 0) {
                // If not the first child, need to figure out the width of
                // output types from all the preceding namespaces
                val ancestorScope: SqlValidatorScope = resolve.scope
                assert(ancestorScope is ListScope)
                val children: List<SqlValidatorNamespace> = (ancestorScope as ListScope).getChildren()
                for (j in 0 until childNamespaceIndex) {
                    namespaceOffset += children[j].getRowType().getFieldCount()
                }
            }
            val field: RelDataTypeField = requireNonNull(
                nameMatcher.field(rowType, originalFieldName)
            ) {
                ("field " + originalFieldName + " is not found in " + rowType
                        + " with " + nameMatcher)
            }
            val origPos: Int = namespaceOffset + field.getIndex()
            groupAnalyzer.groupExprProjection.put(origPos, ref)
        }
        return ImmutableBitSet.of(ref)
    }

    private fun lookupGroupExpr(
        groupAnalyzer: GroupAnalyzer,
        expr: SqlNode
    ): Int {
        for (node in Ord.zip(groupAnalyzer.groupExprs)) {
            if (node.e.equalsDeep(expr, Litmus.IGNORE)) {
                return node.i
            }
        }
        when (expr.getKind()) {
            HOP, TUMBLE, SESSION -> groupAnalyzer.extraExprs.add(expr)
            else -> {}
        }
        groupAnalyzer.groupExprs.add(expr)
        return groupAnalyzer.groupExprs.size() - 1
    }

    /** Computes the rollup of bit sets.
     *
     *
     * For example, `rollup({0}, {1})`
     * returns `({0, 1}, {0}, {})`.
     *
     *
     * Bit sets are not necessarily singletons:
     * `rollup({0, 2}, {3, 5})`
     * returns `({0, 2, 3, 5}, {0, 2}, {})`.  */
    @VisibleForTesting
    fun rollup(
        bitSets: List<ImmutableBitSet?>
    ): ImmutableList<ImmutableBitSet> {
        var bitSets: List<ImmutableBitSet?> = bitSets
        val builder: Set<ImmutableBitSet> = LinkedHashSet()
        while (true) {
            val union: ImmutableBitSet = ImmutableBitSet.union(bitSets)
            builder.add(union)
            if (union.isEmpty()) {
                break
            }
            bitSets = bitSets.subList(0, bitSets.size() - 1)
        }
        return ImmutableList.copyOf(builder)
    }

    /** Computes the cube of bit sets.
     *
     *
     * For example,  `rollup({0}, {1})`
     * returns `({0, 1}, {0}, {})`.
     *
     *
     * Bit sets are not necessarily singletons:
     * `rollup({0, 2}, {3, 5})`
     * returns `({0, 2, 3, 5}, {0, 2}, {})`.  */
    @VisibleForTesting
    fun cube(
        bitSets: List<ImmutableBitSet?>
    ): ImmutableList<ImmutableBitSet> {
        // Given the bit sets [{1}, {2, 3}, {5}],
        // form the lists [[{1}, {}], [{2, 3}, {}], [{5}, {}]].
        val builder: Set<List<ImmutableBitSet>> = LinkedHashSet()
        for (bitSet in bitSets) {
            builder.add(Arrays.asList(bitSet, ImmutableBitSet.of()))
        }
        val flattenedBitSets: Set<ImmutableBitSet> = LinkedHashSet()
        for (o in Linq4j.product(builder)) {
            flattenedBitSets.add(ImmutableBitSet.union(o))
        }
        return ImmutableList.copyOf(flattenedBitSets)
    }

    /**
     * Finds a [org.apache.calcite.jdbc.CalciteSchema.TypeEntry] in a
     * given schema whose type has the given name, possibly qualified.
     *
     * @param rootSchema root schema
     * @param typeName name of the type, may be qualified or fully-qualified
     *
     * @return TypeEntry with a table with the given name, or null
     */
    fun getTypeEntry(
        rootSchema: CalciteSchema, typeName: SqlIdentifier
    ): @Nullable CalciteSchema.TypeEntry? {
        val name: String
        val path: List<String>
        if (typeName.isSimple()) {
            path = ImmutableList.of()
            name = typeName.getSimple()
        } else {
            path = Util.skipLast(typeName.names)
            name = Util.last(typeName.names)
        }
        var schema: CalciteSchema = rootSchema
        for (p in path) {
            if (schema === rootSchema
                && SqlNameMatchers.withCaseSensitive(true).matches(p, schema.getName())
            ) {
                continue
            }
            schema = schema.getSubSchema(p, true)
            if (schema == null) {
                return null
            }
        }
        return schema.getType(name, false)
    }

    /**
     * Finds a [org.apache.calcite.jdbc.CalciteSchema.TableEntry] in a
     * given catalog reader whose table has the given name, possibly qualified.
     *
     *
     * Uses the case-sensitivity policy of the specified catalog reader.
     *
     *
     * If not found, returns null.
     *
     * @param catalogReader accessor to the table metadata
     * @param names Name of table, may be qualified or fully-qualified
     *
     * @return TableEntry with a table with the given name, or null
     */
    fun getTableEntry(
        catalogReader: SqlValidatorCatalogReader, names: List<String?>?
    ): @Nullable CalciteSchema.TableEntry? {
        // First look in the default schema, if any.
        // If not found, look in the root schema.
        for (schemaPath in catalogReader.getSchemaPaths()) {
            val schema: CalciteSchema = getSchema(
                catalogReader.getRootSchema(),
                Iterables.concat(schemaPath, Util.skipLast(names)),
                catalogReader.nameMatcher()
            ) ?: continue
            val entry: CalciteSchema.TableEntry? = getTableEntryFrom(
                schema, Util.last(names),
                catalogReader.nameMatcher().isCaseSensitive()
            )
            if (entry != null) {
                return entry
            }
        }
        return null
    }

    /**
     * Finds and returns [CalciteSchema] nested to the given rootSchema
     * with specified schemaPath.
     *
     *
     * Uses the case-sensitivity policy of specified nameMatcher.
     *
     *
     * If not found, returns null.
     *
     * @param rootSchema root schema
     * @param schemaPath full schema path of required schema
     * @param nameMatcher name matcher
     *
     * @return CalciteSchema that corresponds specified schemaPath
     */
    @Nullable
    fun getSchema(
        rootSchema: CalciteSchema,
        schemaPath: Iterable<String?>, nameMatcher: SqlNameMatcher
    ): CalciteSchema? {
        var schema: CalciteSchema = rootSchema
        for (schemaName in schemaPath) {
            if (schema === rootSchema
                && nameMatcher.matches(schemaName, schema.getName())
            ) {
                continue
            }
            schema = schema.getSubSchema(
                schemaName,
                nameMatcher.isCaseSensitive()
            )
            if (schema == null) {
                return null
            }
        }
        return schema
    }

    private fun getTableEntryFrom(
        schema: CalciteSchema, name: String, caseSensitive: Boolean
    ): @Nullable CalciteSchema.TableEntry? {
        var entry: CalciteSchema.TableEntry = schema.getTable(name, caseSensitive)
        if (entry == null) {
            entry = schema.getTableBasedOnNullaryFunction(
                name,
                caseSensitive
            )
        }
        return entry
    }

    /**
     * Returns whether there are any input columns that are sorted.
     *
     *
     * If so, it can be the default ORDER BY clause for a WINDOW specification.
     * (This is an extension to the SQL standard for streaming.)
     */
    fun containsMonotonic(scope: SqlValidatorScope): Boolean {
        for (ns in children(scope)) {
            ns = ns.resolve()
            for (field in ns.getRowType().getFieldNames()) {
                val monotonicity: SqlMonotonicity = ns.getMonotonicity(field)
                if (monotonicity != null && !monotonicity.mayRepeat()) {
                    return true
                }
            }
        }
        return false
    }

    private fun children(scope: SqlValidatorScope): List<SqlValidatorNamespace> {
        return if (scope is ListScope) (scope as ListScope).getChildren() else ImmutableList.of()
    }

    /**
     * Returns whether any of the given expressions are sorted.
     *
     *
     * If so, it can be the default ORDER BY clause for a WINDOW specification.
     * (This is an extension to the SQL standard for streaming.)
     */
    fun containsMonotonic(scope: SelectScope, nodes: SqlNodeList): Boolean {
        for (node in nodes) {
            if (!scope.getMonotonicity(node).mayRepeat()) {
                return true
            }
        }
        return false
    }

    /**
     * Lookup sql function by sql identifier and function category.
     *
     * @param opTab    operator table to look up
     * @param funName  function name
     * @param funcType function category
     * @return A sql function if and only if there is one operator matches, else null
     */
    @Nullable
    fun lookupSqlFunctionByID(
        opTab: SqlOperatorTable,
        funName: SqlIdentifier,
        @Nullable funcType: SqlFunctionCategory?
    ): SqlOperator? {
        if (funName.isSimple()) {
            val list: List<SqlOperator> = ArrayList()
            opTab.lookupOperatorOverloads(
                funName, funcType, SqlSyntax.FUNCTION, list,
                SqlNameMatchers.withCaseSensitive(funName.isComponentQuoted(0))
            )
            if (list.size() === 1) {
                return list[0]
            }
        }
        return null
    }

    /**
     * Validate the sql node with specified base table row type. For "base table", we mean the
     * table that the sql node expression references fields with.
     *
     * @param caseSensitive whether to match the catalog case-sensitively
     * @param operatorTable operator table
     * @param typeFactory   type factory
     * @param rowType       the table row type that has fields referenced by the expression
     * @param expr          the expression to validate
     * @return pair of a validated expression sql node and its data type,
     * usually a SqlUnresolvedFunction is converted to a resolved function
     */
    fun validateExprWithRowType(
        caseSensitive: Boolean,
        operatorTable: SqlOperatorTable?,
        typeFactory: RelDataTypeFactory,
        rowType: RelDataType?,
        expr: SqlNode
    ): Pair<SqlNode, RelDataType> {
        val tableName = "_table_"
        val select0 = SqlSelect(
            SqlParserPos.ZERO, null,
            SqlNodeList(Collections.singletonList(expr), SqlParserPos.ZERO),
            SqlIdentifier(tableName, SqlParserPos.ZERO),
            null, null, null, null, null, null, null, null
        )
        val catalogReader: CatalogReader = createSingleTableCatalogReader(
            caseSensitive,
            tableName,
            typeFactory,
            rowType
        )
        val validator: SqlValidator = newValidator(
            operatorTable,
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT
        )
        val select: SqlSelect = validator.validate(select0) as SqlSelect
        val selectList: SqlNodeList = select.getSelectList()
        assert(selectList.size() === 1) { "Expression $expr should be atom expression" }
        val node: SqlNode = selectList.get(0)
        val nodeType: RelDataType = validator
            .getValidatedNodeType(select)
            .getFieldList()
            .get(0).getType()
        return Pair.of(node, nodeType)
    }

    /**
     * Creates a catalog reader that contains a single [Table] with temporary table name
     * and specified `rowType`.
     *
     *
     * Make this method public so that other systems can also use it.
     *
     * @param caseSensitive whether to match case sensitively
     * @param tableName     table name to register with
     * @param typeFactory   type factory
     * @param rowType       table row type
     * @return the [CalciteCatalogReader] instance
     */
    fun createSingleTableCatalogReader(
        caseSensitive: Boolean,
        tableName: String?,
        typeFactory: RelDataTypeFactory?,
        rowType: RelDataType?
    ): CalciteCatalogReader {
        // connection properties
        val properties = Properties()
        properties.put(
            CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(caseSensitive)
        )
        val connectionConfig: CalciteConnectionConfig = CalciteConnectionConfigImpl(properties)

        // prepare root schema
        val table = ExplicitRowTypeTable(rowType)
        val tableMap: Map<String?, Table> = Collections.singletonMap(tableName, table)
        val schema: CalciteSchema = CalciteSchema.createRootSchema(
            false,
            false,
            "",
            ExplicitTableSchema(tableMap)
        )
        return CalciteCatalogReader(
            schema,
            ArrayList(ArrayList()),
            typeFactory,
            connectionConfig
        )
    }

    /**
     * Flattens an aggregate call.
     */
    fun flatten(call: SqlCall): FlatAggregate {
        return flattenRecurse(null, null, null, call)
    }

    private fun flattenRecurse(
        @Nullable filterCall: SqlCall?,
        @Nullable distinctCall: SqlCall?, @Nullable orderCall: SqlCall?,
        call: SqlCall
    ): FlatAggregate {
        return when (call.getKind()) {
            FILTER -> {
                assert(filterCall == null)
                flattenRecurse(
                    call,
                    distinctCall,
                    orderCall,
                    call.operand(0)
                )
            }
            WITHIN_DISTINCT -> {
                assert(distinctCall == null)
                flattenRecurse(
                    filterCall,
                    call,
                    orderCall,
                    call.operand(0)
                )
            }
            WITHIN_GROUP -> {
                assert(orderCall == null)
                flattenRecurse(
                    filterCall,
                    distinctCall,
                    call,
                    call.operand(0)
                )
            }
            else -> FlatAggregate(
                call,
                filterCall,
                distinctCall,
                orderCall
            )
        }
    }

    val EXPR_SUGGESTER: Suggester = Suggester { original: String?, attempt: Int, size: Int ->
        Util.first(
            original,
            SqlUtil.GENERATED_EXPR_ALIAS_PREFIX
        ) + attempt
    }
    val F_SUGGESTER: Suggester = Suggester { original: String?, attempt: Int, size: Int ->
        (Util.first(original, "\$f")
                + Math.max(size, attempt))
    }
    val ATTEMPT_SUGGESTER: Suggester =
        Suggester { original: String?, attempt: Int, size: Int -> Util.first(original, "$") + attempt }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Walks over an expression, copying every node, and fully-qualifying every
     * identifier.
     */
    @Deprecated // to be removed before 2.0
    class DeepCopier internal constructor(scope: SqlValidatorScope?) : SqlScopedShuttle(scope) {
        @Override
        override fun visit(list: SqlNodeList): SqlNode {
            val copy = SqlNodeList(list.getParserPosition())
            for (node in list) {
                copy.add(node.accept(this))
            }
            return copy
        }

        // Override to copy all arguments regardless of whether visitor changes
        // them.
        @Override
        protected override fun visitScoped(call: SqlCall): SqlNode {
            val argHandler = CallCopyingArgHandler(call, true)
            call.getOperator().acceptCall(this, call, false, argHandler)
            return argHandler.result()
        }

        @Override
        override fun visit(literal: SqlLiteral?): SqlNode {
            return SqlNode.clone(literal)
        }

        @Override
        override fun visit(id: SqlIdentifier?): SqlNode {
            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            val validator: SqlValidator = getScope().getValidator()
            val call: SqlCall = validator.makeNullaryCall(id)
            return if (call != null) {
                call
            } else getScope().fullyQualify(id).identifier
        }

        @Override
        override fun visit(type: SqlDataTypeSpec?): SqlNode {
            return SqlNode.clone(type)
        }

        @Override
        override fun visit(param: SqlDynamicParam?): SqlNode {
            return SqlNode.clone(param)
        }

        @Override
        override fun visit(intervalQualifier: SqlIntervalQualifier?): SqlNode {
            return SqlNode.clone(intervalQualifier)
        }

        companion object {
            /** Copies a list of nodes.  */
            @Nullable
            fun copy(scope: SqlValidatorScope?, list: SqlNodeList): SqlNodeList {
                return list.accept(DeepCopier(scope)) as SqlNodeList
            }
        }
    }

    /** Suggests candidates for unique names, given the number of attempts so far
     * and the number of expressions in the project list.  */
    interface Suggester {
        fun apply(@Nullable original: String?, attempt: Int, size: Int): String?
    }

    /** Builds a list of GROUP BY expressions.  */
    class GroupAnalyzer {
        /** Extra expressions, computed from the input as extra GROUP BY
         * expressions. For example, calls to the `TUMBLE` functions.  */
        val extraExprs: List<SqlNode> = ArrayList()
        val groupExprs: List<SqlNode> = ArrayList()
        val groupExprProjection: Map<Integer, Integer> = HashMap()
    }

    /**
     * A [AbstractTable] that can specify the row type explicitly.
     */
    private class ExplicitRowTypeTable internal constructor(rowType: RelDataType?) : AbstractTable() {
        private val rowType: RelDataType

        init {
            this.rowType = requireNonNull(rowType, "rowType")
        }

        @Override
        fun getRowType(typeFactory: RelDataTypeFactory?): RelDataType {
            return rowType
        }
    }

    /**
     * A [AbstractSchema] that can specify the table map explicitly.
     */
    private class ExplicitTableSchema internal constructor(tableMap: Map<String?, Table?>?) : AbstractSchema() {
        private val tableMap: Map<String, Table>

        init {
            this.tableMap = requireNonNull(tableMap, "tableMap")
        }

        @Override
        protected fun getTableMap(): Map<String, Table> {
            return tableMap
        }
    }

    /** Flattens any FILTER, WITHIN DISTINCT, WITHIN GROUP surrounding a call to
     * an aggregate function.  */
    class FlatAggregate internal constructor(
        aggregateCall: SqlCall?, @Nullable filterCall: SqlCall?,
        @Nullable distinctCall: SqlCall?, @Nullable orderCall: SqlCall?
    ) {
        val aggregateCall: SqlCall

        @Nullable
        val filterCall: SqlCall?

        @Nullable
        val filter: SqlNode?

        @Nullable
        val distinctCall: SqlCall?

        @Nullable
        val distinctList: SqlNodeList?

        @Nullable
        val orderCall: SqlCall?

        @Nullable
        val orderList: SqlNodeList?

        init {
            this.aggregateCall = Objects.requireNonNull(aggregateCall, "aggregateCall")
            Preconditions.checkArgument(
                filterCall == null
                        || filterCall.getKind() === SqlKind.FILTER
            )
            Preconditions.checkArgument(
                distinctCall == null
                        || distinctCall.getKind() === SqlKind.WITHIN_DISTINCT
            )
            Preconditions.checkArgument(
                orderCall == null
                        || orderCall.getKind() === SqlKind.WITHIN_GROUP
            )
            this.filterCall = filterCall
            filter = if (filterCall == null) null else filterCall.operand(1)
            this.distinctCall = distinctCall
            distinctList = if (distinctCall == null) null else distinctCall.operand(1)
            this.orderCall = orderCall
            orderList = if (orderCall == null) null else orderCall.operand(1)
        }
    }
}
