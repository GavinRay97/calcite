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
package org.apache.calcite.sql2rel

import org.apache.calcite.linq4j.Ord

/**
 * Transformer that walks over a tree of relational expressions, replacing each
 * [RelNode] with a 'slimmed down' relational expression that projects
 * only the columns required by its consumer.
 *
 *
 * Uses multi-methods to fire the right rule for each type of relational
 * expression. This allows the transformer to be extended without having to
 * add a new method to RelNode, and without requiring a collection of rule
 * classes scattered to the four winds.
 *
 *
 * REVIEW: jhyde, 2009/7/28: Is sql2rel the correct package for this class?
 * Trimming fields is not an essential part of SQL-to-Rel translation, and
 * arguably belongs in the optimization phase. But this transformer does not
 * obey the usual pattern for planner rules; it is difficult to do so, because
 * each [RelNode] needs to return a different set of fields after
 * trimming.
 *
 *
 * TODO: Change 2nd arg of the [.trimFields] method from BitSet to
 * Mapping. Sometimes it helps the consumer if you return the columns in a
 * particular order. For instance, it may avoid a project at the top of the
 * tree just for reordering. Could ease the transition by writing methods that
 * convert BitSet to Mapping and vice versa.
 */
class RelFieldTrimmer(@Nullable validator: SqlValidator?, relBuilder: RelBuilder) : ReflectiveVisitor {
    //~ Static fields/initializers ---------------------------------------------
    //~ Instance fields --------------------------------------------------------
    private val trimFieldsDispatcher: ReflectUtil.MethodDispatcher<TrimResult>
    private val relBuilder: RelBuilder
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RelFieldTrimmer.
     *
     * @param validator Validator
     */
    init {
        Util.discard(validator) // may be useful one day
        this.relBuilder = relBuilder
        @SuppressWarnings("argument.type.incompatible") val dispatcher: ReflectUtil.MethodDispatcher<TrimResult> =
            ReflectUtil.createMethodDispatcher(
                TrimResult::class.java,
                this,
                "trimFields",
                RelNode::class.java,
                ImmutableBitSet::class.java,
                Set::class.java
            )
        trimFieldsDispatcher = dispatcher
    }

    @Deprecated // to be removed before 2.0
    constructor(
        @Nullable validator: SqlValidator?,
        cluster: RelOptCluster?,
        projectFactory: RelFactories.ProjectFactory?,
        filterFactory: FilterFactory?,
        joinFactory: RelFactories.JoinFactory?,
        sortFactory: RelFactories.SortFactory?,
        aggregateFactory: RelFactories.AggregateFactory?,
        setOpFactory: RelFactories.SetOpFactory?
    ) : this(
        validator,
        RelBuilder.proto(
            projectFactory, filterFactory, joinFactory,
            sortFactory, aggregateFactory, setOpFactory
        )
            .create(cluster, null)
    ) {
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Trims unused fields from a relational expression.
     *
     *
     * We presume that all fields of the relational expression are wanted by
     * its consumer, so only trim fields that are not used within the tree.
     *
     * @param root Root node of relational expression
     * @return Trimmed relational expression
     */
    fun trim(root: RelNode): RelNode {
        val fieldCount: Int = root.getRowType().getFieldCount()
        val fieldsUsed: ImmutableBitSet = ImmutableBitSet.range(fieldCount)
        val extraFields: Set<RelDataTypeField> = Collections.emptySet()
        val trimResult = dispatchTrimFields(root, fieldsUsed, extraFields)
        if (!trimResult.right.isIdentity()) {
            throw IllegalArgumentException()
        }
        if (SqlToRelConverter.SQL2REL_LOGGER.isDebugEnabled()) {
            SqlToRelConverter.SQL2REL_LOGGER.debug(
                RelOptUtil.dumpPlan(
                    "Plan after trimming unused fields",
                    trimResult.left, SqlExplainFormat.TEXT,
                    SqlExplainLevel.EXPPLAN_ATTRIBUTES
                )
            )
        }
        return trimResult.left
    }

    /**
     * Trims the fields of an input relational expression.
     *
     * @param rel        Relational expression
     * @param input      Input relational expression, whose fields to trim
     * @param fieldsUsed Bitmap of fields needed by the consumer
     * @return New relational expression and its field mapping
     */
    protected fun trimChild(
        rel: RelNode,
        input: RelNode,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>
    ): TrimResult {
        val fieldsUsedBuilder: ImmutableBitSet.Builder = fieldsUsed.rebuild()

        // Fields that define the collation cannot be discarded.
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        val collations: ImmutableList<RelCollation> = mq.collations(input)
        if (collations != null) {
            for (collation in collations) {
                for (fieldCollation in collation.getFieldCollations()) {
                    fieldsUsedBuilder.set(fieldCollation.getFieldIndex())
                }
            }
        }

        // Correlating variables are a means for other relational expressions to use
        // fields.
        for (correlation in rel.getVariablesSet()) {
            rel.accept(
                object : CorrelationReferenceFinder() {
                    @Override
                    protected fun handle(fieldAccess: RexFieldAccess): RexNode {
                        val v: RexCorrelVariable = fieldAccess.getReferenceExpr() as RexCorrelVariable
                        if (v.id.equals(correlation)) {
                            fieldsUsedBuilder.set(fieldAccess.getField().getIndex())
                        }
                        return fieldAccess
                    }
                })
        }
        return dispatchTrimFields(input, fieldsUsedBuilder.build(), extraFields)
    }

    /**
     * Trims a child relational expression, then adds back a dummy project to
     * restore the fields that were removed.
     *
     *
     * Sounds pointless? It causes unused fields to be removed
     * further down the tree (towards the leaves), but it ensure that the
     * consuming relational expression continues to see the same fields.
     *
     * @param rel        Relational expression
     * @param input      Input relational expression, whose fields to trim
     * @param fieldsUsed Bitmap of fields needed by the consumer
     * @return New relational expression and its field mapping
     */
    protected fun trimChildRestore(
        rel: RelNode,
        input: RelNode,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>
    ): TrimResult {
        val trimResult = trimChild(rel, input, fieldsUsed, extraFields)
        if (trimResult.right.isIdentity()) {
            return trimResult
        }
        val rowType: RelDataType = input.getRowType()
        val fieldList: List<RelDataTypeField> = rowType.getFieldList()
        val exprList: List<RexNode> = ArrayList()
        val nameList: List<String> = rowType.getFieldNames()
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        assert(trimResult.right.getSourceCount() === fieldList.size())
        for (i in 0 until fieldList.size()) {
            val source: Int = trimResult.right.getTargetOpt(i)
            val field: RelDataTypeField = fieldList[i]
            exprList.add(
                if (source < 0) rexBuilder.makeZeroLiteral(field.getType()) else rexBuilder.makeInputRef(
                    field.getType(),
                    source
                )
            )
        }
        relBuilder.push(trimResult.left)
            .project(exprList, nameList)
        return result(
            relBuilder.build(),
            Mappings.createIdentity(fieldList.size())
        )
    }

    /**
     * Invokes [.trimFields], or the appropriate method for the type
     * of the rel parameter, using multi-method dispatch.
     *
     * @param rel        Relational expression
     * @param fieldsUsed Bitmap of fields needed by the consumer
     * @return New relational expression and its field mapping
     */
    protected fun dispatchTrimFields(
        rel: RelNode,
        fieldsUsed: ImmutableBitSet?,
        extraFields: Set<RelDataTypeField?>
    ): TrimResult {
        val trimResult: TrimResult = trimFieldsDispatcher.invoke(rel, fieldsUsed, extraFields)
        val newRel: RelNode = trimResult.left
        val mapping: Mapping = trimResult.right
        val fieldCount: Int = rel.getRowType().getFieldCount()
        assert(mapping.getSourceCount() === fieldCount) {
            "source: " + mapping.getSourceCount().toString() + " != " + fieldCount
        }
        val newFieldCount: Int = newRel.getRowType().getFieldCount()
        assert(
            mapping.getTargetCount() + extraFields.size() === newFieldCount
                    || Bug.TODO_FIXED
        ) {
            "target: " + mapping.getTargetCount()
                .toString() + " + " + extraFields.size()
                .toString() + " != " + newFieldCount
        }
        if (Bug.TODO_FIXED) {
            assert(newFieldCount > 0) { "rel has no fields after trim: $rel" }
        }
        return if (newRel.equals(rel)) {
            result(rel, mapping)
        } else trimResult
    }

    protected fun result(r: RelNode, mapping: Mapping): TrimResult {
        var r: RelNode = r
        val rexBuilder: RexBuilder = relBuilder.getRexBuilder()
        for (correlation in r.getVariablesSet()) {
            r = r.accept(
                object : CorrelationReferenceFinder() {
                    @Override
                    protected fun handle(fieldAccess: RexFieldAccess): RexNode {
                        val v: RexCorrelVariable = fieldAccess.getReferenceExpr() as RexCorrelVariable
                        if (v.id.equals(correlation)
                            && v.getType().getFieldCount() === mapping.getSourceCount()
                        ) {
                            val old: Int = fieldAccess.getField().getIndex()
                            val new_: Int = mapping.getTarget(old)
                            val typeBuilder: RelDataTypeFactory.Builder = relBuilder.getTypeFactory().builder()
                            for (target in Util.range(mapping.getTargetCount())) {
                                typeBuilder.add(
                                    v.getType().getFieldList().get(mapping.getSource(target))
                                )
                            }
                            val newV: RexNode = rexBuilder.makeCorrel(typeBuilder.build(), v.id)
                            if (old != new_) {
                                return rexBuilder.makeFieldAccess(newV, new_)
                            }
                        }
                        return fieldAccess
                    }
                })
        }
        return TrimResult(r, mapping)
    }

    /**
     * Visit method, per [org.apache.calcite.util.ReflectiveVisitor].
     *
     *
     * This method is invoked reflectively, so there may not be any apparent
     * calls to it. The class (or derived classes) may contain overloads of
     * this method with more specific types for the `rel` parameter.
     *
     *
     * Returns a pair: the relational expression created, and the mapping
     * between the original fields and the fields of the newly created
     * relational expression.
     *
     * @param rel        Relational expression
     * @param fieldsUsed Fields needed by the consumer
     * @return relational expression and mapping
     */
    fun trimFields(
        rel: RelNode,
        fieldsUsed: ImmutableBitSet?,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        // We don't know how to trim this kind of relational expression, so give
        // it back intact.
        Util.discard(fieldsUsed)
        return result(
            rel,
            Mappings.createIdentity(rel.getRowType().getFieldCount())
        )
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalCalc].
     */
    fun trimFields(
        calc: Calc,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rexProgram: RexProgram = calc.getProgram()
        val projs: List<RexNode> = Util.transform(
            rexProgram.getProjectList(),
            rexProgram::expandLocalRef
        )
        var conditionExpr: RexNode? = null
        if (rexProgram.getCondition() != null) {
            val filter: List<RexNode> = Util.transform(
                ImmutableList.of(
                    rexProgram.getCondition()
                ), rexProgram::expandLocalRef
            )
            assert(filter.size() === 1)
            conditionExpr = filter[0]
        }
        val rowType: RelDataType = calc.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val input: RelNode = calc.getInput()
        val inputExtraFields: Set<RelDataTypeField> = HashSet(extraFields)
        val inputFinder: RelOptUtil.InputFinder = InputFinder(inputExtraFields)
        for (ord in Ord.zip(projs)) {
            if (fieldsUsed.get(ord.i)) {
                ord.e.accept(inputFinder)
            }
        }
        if (conditionExpr != null) {
            conditionExpr.accept(inputFinder)
        }
        val inputFieldsUsed: ImmutableBitSet = inputFinder.build()

        // Create input with trimmed columns.
        val trimResult = trimChild(calc, input, inputFieldsUsed, inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right

        // If the input is unchanged, and we need to project all columns,
        // there's nothing we can do.
        if (newInput === input
            && fieldsUsed.cardinality() === fieldCount
        ) {
            return result(calc, Mappings.createIdentity(fieldCount))
        }

        // Some parts of the system can't handle rows with zero fields, so
        // pretend that one field is used.
        if (fieldsUsed.cardinality() === 0 && rexProgram.getCondition() == null) {
            return dummyProject(fieldCount, newInput)
        }

        // Build new project expressions, and populate the mapping.
        val newProjects: List<RexNode> = ArrayList()
        val shuttle: RexVisitor<RexNode> = RexPermuteInputsShuttle(
            inputMapping, newInput
        )
        val mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality()
        )
        for (ord in Ord.zip(projs)) {
            if (fieldsUsed.get(ord.i)) {
                mapping.set(ord.i, newProjects.size())
                val newProjectExpr: RexNode = ord.e.accept(shuttle)
                newProjects.add(newProjectExpr)
            }
        }
        val newRowType: RelDataType = RelOptUtil.permute(
            calc.getCluster().getTypeFactory(), rowType,
            mapping
        )
        val newInputRelNode: RelNode = relBuilder.push(newInput).build()
        var newConditionExpr: RexNode? = null
        if (conditionExpr != null) {
            newConditionExpr = conditionExpr.accept(shuttle)
        }
        val newRexProgram: RexProgram = RexProgram.create(
            newInputRelNode.getRowType(),
            newProjects, newConditionExpr, newRowType.getFieldNames(),
            newInputRelNode.getCluster().getRexBuilder()
        )
        val newCalc: Calc = calc.copy(calc.getTraitSet(), newInputRelNode, newRexProgram)
        return result(newCalc, mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalProject].
     */
    fun trimFields(
        project: Project,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rowType: RelDataType = project.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val input: RelNode = project.getInput()

        // Which fields are required from the input?
        val inputExtraFields: Set<RelDataTypeField> = LinkedHashSet(extraFields)
        val inputFinder: RelOptUtil.InputFinder = InputFinder(inputExtraFields)
        for (ord in Ord.zip(project.getProjects())) {
            if (fieldsUsed.get(ord.i)) {
                ord.e.accept(inputFinder)
            }
        }
        val inputFieldsUsed: ImmutableBitSet = inputFinder.build()

        // Create input with trimmed columns.
        val trimResult = trimChild(project, input, inputFieldsUsed, inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right

        // If the input is unchanged, and we need to project all columns,
        // there's nothing we can do.
        if (newInput === input
            && fieldsUsed.cardinality() === fieldCount
        ) {
            return result(project, Mappings.createIdentity(fieldCount))
        }

        // Some parts of the system can't handle rows with zero fields, so
        // pretend that one field is used.
        if (fieldsUsed.cardinality() === 0) {
            return dummyProject(fieldCount, newInput, project)
        }

        // Build new project expressions, and populate the mapping.
        val newProjects: List<RexNode> = ArrayList()
        val shuttle: RexVisitor<RexNode> = RexPermuteInputsShuttle(
            inputMapping, newInput
        )
        val mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality()
        )
        for (ord in Ord.zip(project.getProjects())) {
            if (fieldsUsed.get(ord.i)) {
                mapping.set(ord.i, newProjects.size())
                val newProjectExpr: RexNode = ord.e.accept(shuttle)
                newProjects.add(newProjectExpr)
            }
        }
        val newRowType: RelDataType = RelOptUtil.permute(
            project.getCluster().getTypeFactory(), rowType,
            mapping
        )
        relBuilder.push(newInput)
        relBuilder.project(newProjects, newRowType.getFieldNames())
        val newProject: RelNode = RelOptUtil.propagateRelHints(project, relBuilder.build())
        return result(newProject, mapping)
    }

    /** Creates a project with a dummy column, to protect the parts of the system
     * that cannot handle a relational expression with no columns.
     *
     * @param fieldCount Number of fields in the original relational expression
     * @param input Trimmed input
     * @return Dummy project
     */
    protected fun dummyProject(fieldCount: Int, input: RelNode): TrimResult {
        return dummyProject(fieldCount, input, null)
    }

    /** Creates a project with a dummy column, to protect the parts of the system
     * that cannot handle a relational expression with no columns.
     *
     * @param fieldCount Number of fields in the original relational expression
     * @param input Trimmed input
     * @param originalRelNode Source RelNode for hint propagation (or null if no propagation needed)
     * @return Dummy project
     */
    protected fun dummyProject(
        fieldCount: Int, input: RelNode,
        @Nullable originalRelNode: RelNode?
    ): TrimResult {
        val cluster: RelOptCluster = input.getCluster()
        val mapping: Mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, 1)
        if (input.getRowType().getFieldCount() === 1) {
            // Input already has one field (and may in fact be a dummy project we
            // created for the child). We can't do better.
            return result(input, mapping)
        }
        val expr: RexLiteral = cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO)
        relBuilder.push(input)
        relBuilder.project(ImmutableList.of(expr), ImmutableList.of("DUMMY"))
        var newProject: RelNode = relBuilder.build()
        if (originalRelNode != null) {
            newProject = RelOptUtil.propagateRelHints(originalRelNode, newProject)
        }
        return result(newProject, mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalFilter].
     */
    fun trimFields(
        filter: Filter,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rowType: RelDataType = filter.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val conditionExpr: RexNode = filter.getCondition()
        val input: RelNode = filter.getInput()

        // We use the fields used by the consumer, plus any fields used in the
        // filter.
        val inputExtraFields: Set<RelDataTypeField> = LinkedHashSet(extraFields)
        val inputFinder: RelOptUtil.InputFinder = InputFinder(inputExtraFields, fieldsUsed)
        conditionExpr.accept(inputFinder)
        val inputFieldsUsed: ImmutableBitSet = inputFinder.build()

        // Create input with trimmed columns.
        val trimResult = trimChild(filter, input, inputFieldsUsed, inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right

        // If the input is unchanged, and we need to project all columns,
        // there's nothing we can do.
        if (newInput === input
            && fieldsUsed.cardinality() === fieldCount
        ) {
            return result(filter, Mappings.createIdentity(fieldCount))
        }

        // Build new project expressions, and populate the mapping.
        val shuttle: RexVisitor<RexNode> = RexPermuteInputsShuttle(inputMapping, newInput)
        val newConditionExpr: RexNode = conditionExpr.accept(shuttle)

        // Build new filter with trimmed input and condition.
        relBuilder.push(newInput)
            .filter(filter.getVariablesSet(), newConditionExpr)

        // The result has the same mapping as the input gave us. Sometimes we
        // return fields that the consumer didn't ask for, because the filter
        // needs them for its condition.
        return result(relBuilder.build(), inputMapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.core.Sort].
     */
    fun trimFields(
        sort: Sort,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rowType: RelDataType = sort.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val collation: RelCollation = sort.getCollation()
        val input: RelNode = sort.getInput()

        // We use the fields used by the consumer, plus any fields used as sort
        // keys.
        val inputFieldsUsed: ImmutableBitSet.Builder = fieldsUsed.rebuild()
        for (field in collation.getFieldCollations()) {
            inputFieldsUsed.set(field.getFieldIndex())
        }

        // Create input with trimmed columns.
        val inputExtraFields: Set<RelDataTypeField> = Collections.emptySet()
        val trimResult = trimChild(sort, input, inputFieldsUsed.build(), inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right

        // If the input is unchanged, and we need to project all columns,
        // there's nothing we can do.
        if (newInput === input && inputMapping.isIdentity()
            && fieldsUsed.cardinality() === fieldCount
        ) {
            return result(sort, Mappings.createIdentity(fieldCount))
        }

        // leave the Sort unchanged in case we have dynamic limits
        if (sort.offset is RexDynamicParam
            || sort.fetch is RexDynamicParam
        ) {
            return result(sort, inputMapping)
        }
        relBuilder.push(newInput)
        val offset = if (sort.offset == null) 0 else RexLiteral.intValue(sort.offset)
        val fetch = if (sort.fetch == null) -1 else RexLiteral.intValue(sort.fetch)
        val fields: ImmutableList<RexNode> = relBuilder.fields(RexUtil.apply(inputMapping, collation))
        relBuilder.sortLimit(offset, fetch, fields)

        // The result has the same mapping as the input gave us. Sometimes we
        // return fields that the consumer didn't ask for, because the filter
        // needs them for its condition.
        return result(relBuilder.build(), inputMapping)
    }

    fun trimFields(
        exchange: Exchange,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rowType: RelDataType = exchange.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val distribution: RelDistribution = exchange.getDistribution()
        val input: RelNode = exchange.getInput()

        // We use the fields used by the consumer, plus any fields used as exchange
        // keys.
        val inputFieldsUsed: ImmutableBitSet.Builder = fieldsUsed.rebuild()
        for (keyIndex in distribution.getKeys()) {
            inputFieldsUsed.set(keyIndex)
        }

        // Create input with trimmed columns.
        val inputExtraFields: Set<RelDataTypeField> = Collections.emptySet()
        val trimResult = trimChild(exchange, input, inputFieldsUsed.build(), inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right

        // If the input is unchanged, and we need to project all columns,
        // there's nothing we can do.
        if (newInput === input && inputMapping.isIdentity()
            && fieldsUsed.cardinality() === fieldCount
        ) {
            return result(exchange, Mappings.createIdentity(fieldCount))
        }
        relBuilder.push(newInput)
        val newDistribution: RelDistribution = distribution.apply(inputMapping)
        relBuilder.exchange(newDistribution)
        return result(relBuilder.build(), inputMapping)
    }

    fun trimFields(
        sortExchange: SortExchange,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rowType: RelDataType = sortExchange.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val collation: RelCollation = sortExchange.getCollation()
        val distribution: RelDistribution = sortExchange.getDistribution()
        val input: RelNode = sortExchange.getInput()

        // We use the fields used by the consumer, plus any fields used as sortExchange
        // keys.
        val inputFieldsUsed: ImmutableBitSet.Builder = fieldsUsed.rebuild()
        for (field in collation.getFieldCollations()) {
            inputFieldsUsed.set(field.getFieldIndex())
        }
        for (keyIndex in distribution.getKeys()) {
            inputFieldsUsed.set(keyIndex)
        }

        // Create input with trimmed columns.
        val inputExtraFields: Set<RelDataTypeField> = Collections.emptySet()
        val trimResult = trimChild(sortExchange, input, inputFieldsUsed.build(), inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right

        // If the input is unchanged, and we need to project all columns,
        // there's nothing we can do.
        if (newInput === input && inputMapping.isIdentity()
            && fieldsUsed.cardinality() === fieldCount
        ) {
            return result(sortExchange, Mappings.createIdentity(fieldCount))
        }
        relBuilder.push(newInput)
        val newCollation: RelCollation = RexUtil.apply(inputMapping, collation)
        val newDistribution: RelDistribution = distribution.apply(inputMapping)
        relBuilder.sortExchange(newDistribution, newCollation)
        return result(relBuilder.build(), inputMapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalJoin].
     */
    fun trimFields(
        join: Join,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val fieldCount: Int = (join.getSystemFieldList().size()
                + join.getLeft().getRowType().getFieldCount()
                + join.getRight().getRowType().getFieldCount())
        val conditionExpr: RexNode = join.getCondition()
        val systemFieldCount: Int = join.getSystemFieldList().size()

        // Add in fields used in the condition.
        val combinedInputExtraFields: Set<RelDataTypeField> = LinkedHashSet(extraFields)
        val inputFinder: RelOptUtil.InputFinder = InputFinder(combinedInputExtraFields, fieldsUsed)
        conditionExpr.accept(inputFinder)
        val fieldsUsedPlus: ImmutableBitSet = inputFinder.build()

        // If no system fields are used, we can remove them.
        var systemFieldUsedCount = 0
        for (i in 0 until systemFieldCount) {
            if (fieldsUsed.get(i)) {
                ++systemFieldUsedCount
            }
        }
        val newSystemFieldCount: Int
        newSystemFieldCount = if (systemFieldUsedCount == 0) {
            0
        } else {
            systemFieldCount
        }
        var offset = systemFieldCount
        var changeCount = 0
        var newFieldCount = newSystemFieldCount
        val newInputs: List<RelNode> = ArrayList(2)
        val inputMappings: List<Mapping> = ArrayList()
        val inputExtraFieldCounts: List<Integer> = ArrayList()
        for (input in join.getInputs()) {
            val inputRowType: RelDataType = input.getRowType()
            val inputFieldCount: Int = inputRowType.getFieldCount()

            // Compute required mapping.
            val inputFieldsUsed: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (bit in fieldsUsedPlus) {
                if (bit >= offset && bit < offset + inputFieldCount) {
                    inputFieldsUsed.set(bit - offset)
                }
            }

            // If there are system fields, we automatically use the
            // corresponding field in each input.
            inputFieldsUsed.set(0, newSystemFieldCount)

            // FIXME: We ought to collect extra fields for each input
            // individually. For now, we assume that just one input has
            // on-demand fields.
            val inputExtraFields: Set<RelDataTypeField> =
                if (RelDataTypeImpl.extra(inputRowType) == null) Collections.emptySet() else combinedInputExtraFields
            inputExtraFieldCounts.add(inputExtraFields.size())
            val trimResult = trimChild(join, input, inputFieldsUsed.build(), inputExtraFields)
            newInputs.add(trimResult.left)
            if (trimResult.left !== input) {
                ++changeCount
            }
            val inputMapping: Mapping = trimResult.right
            inputMappings.add(inputMapping)

            // Move offset to point to start of next input.
            offset += inputFieldCount
            newFieldCount += inputMapping.getTargetCount() + inputExtraFields.size()
        }
        var mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            newFieldCount
        )
        for (i in 0 until newSystemFieldCount) {
            mapping.set(i, i)
        }
        offset = systemFieldCount
        var newOffset = newSystemFieldCount
        for (i in 0 until inputMappings.size()) {
            val inputMapping: Mapping = inputMappings[i]
            for (pair in inputMapping) {
                mapping.set(pair.source + offset, pair.target + newOffset)
            }
            offset += inputMapping.getSourceCount()
            newOffset += (inputMapping.getTargetCount()
                    + inputExtraFieldCounts[i])
        }
        if (changeCount == 0
            && mapping.isIdentity()
        ) {
            return result(join, Mappings.createIdentity(fieldCount))
        }

        // Build new join.
        val shuttle: RexVisitor<RexNode> = RexPermuteInputsShuttle(
            mapping, newInputs[0], newInputs[1]
        )
        val newConditionExpr: RexNode = conditionExpr.accept(shuttle)
        relBuilder.push(newInputs[0])
        relBuilder.push(newInputs[1])
        when (join.getJoinType()) {
            SEMI, ANTI -> {
                // For SemiJoins and AntiJoins only map fields from the left-side
                if (join.getJoinType() === JoinRelType.SEMI) {
                    relBuilder.semiJoin(newConditionExpr)
                } else {
                    relBuilder.antiJoin(newConditionExpr)
                }
                val inputMapping: Mapping = inputMappings[0]
                mapping = Mappings.create(
                    MappingType.INVERSE_SURJECTION,
                    join.getRowType().getFieldCount(),
                    newSystemFieldCount + inputMapping.getTargetCount()
                )
                var i = 0
                while (i < newSystemFieldCount) {
                    mapping.set(i, i)
                    ++i
                }
                offset = systemFieldCount
                newOffset = newSystemFieldCount
                for (pair in inputMapping) {
                    mapping.set(pair.source + offset, pair.target + newOffset)
                }
            }
            else -> relBuilder.join(join.getJoinType(), newConditionExpr)
        }
        relBuilder.hints(join.getHints())
        return result(relBuilder.build(), mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.core.SetOp] (Only UNION ALL is supported).
     */
    fun trimFields(
        setOp: SetOp,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>
    ): TrimResult {
        var fieldsUsed: ImmutableBitSet = fieldsUsed
        val rowType: RelDataType = setOp.getRowType()
        val fieldCount: Int = rowType.getFieldCount()

        // Trim fields only for UNION ALL.
        //
        // UNION | INTERSECT | INTERSECT ALL | EXCEPT | EXCEPT ALL
        // all have comparison between branches.
        // They can not be trimmed because the comparison needs
        // complete fields.
        if (!(setOp.kind === SqlKind.UNION && setOp.all)) {
            return result(setOp, Mappings.createIdentity(fieldCount))
        }
        var changeCount = 0

        // Fennel abhors an empty row type, so pretend that the parent rel
        // wants the last field. (The last field is the least likely to be a
        // system field.)
        if (fieldsUsed.isEmpty()) {
            fieldsUsed = ImmutableBitSet.of(rowType.getFieldCount() - 1)
        }

        // Compute the desired field mapping. Give the consumer the fields they
        // want, in the order that they appear in the bitset.
        val mapping: Mapping = createMapping(fieldsUsed, fieldCount)

        // Create input with trimmed columns.
        for (input in setOp.getInputs()) {
            val trimResult = trimChild(setOp, input, fieldsUsed, extraFields)

            // We want "mapping", the input gave us "inputMapping", compute
            // "remaining" mapping.
            //    |                   |                |
            //    |---------------- mapping ---------->|
            //    |-- inputMapping -->|                |
            //    |                   |-- remaining -->|
            //
            // For instance, suppose we have columns [a, b, c, d],
            // the consumer asked for mapping = [b, d],
            // and the transformed input has columns inputMapping = [d, a, b].
            // remaining will permute [b, d] to [d, a, b].
            val remaining: Mapping = Mappings.divide(mapping, trimResult.right)

            // Create a projection; does nothing if remaining is identity.
            relBuilder.push(trimResult.left)
            relBuilder.permute(remaining)
            if (input !== relBuilder.peek()) {
                ++changeCount
            }
        }

        // If the input is unchanged, and we need to project all columns,
        // there's to do.
        if (changeCount == 0
            && mapping.isIdentity()
        ) {
            for (@SuppressWarnings("unused") input in setOp.getInputs()) {
                relBuilder.build()
            }
            return result(setOp, mapping)
        }
        when (setOp.kind) {
            UNION -> relBuilder.union(setOp.all, setOp.getInputs().size())
            INTERSECT -> relBuilder.intersect(setOp.all, setOp.getInputs().size())
            EXCEPT -> {
                assert(setOp.getInputs().size() === 2)
                relBuilder.minus(setOp.all)
            }
            else -> throw AssertionError("unknown setOp $setOp")
        }
        return result(relBuilder.build(), mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalAggregate].
     */
    fun trimFields(
        aggregate: Aggregate,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        // Fields:
        //
        // | sys fields | group fields | indicator fields | agg functions |
        //
        // Two kinds of trimming:
        //
        // 1. If agg rel has system fields but none of these are used, create an
        // agg rel with no system fields.
        //
        // 2. If aggregate functions are not used, remove them.
        //
        // But group and indicator fields stay, even if they are not used.
        var fieldsUsed: ImmutableBitSet = fieldsUsed
        val rowType: RelDataType = aggregate.getRowType()

        // Compute which input fields are used.
        // 1. group fields are always used
        val inputFieldsUsed: ImmutableBitSet.Builder = aggregate.getGroupSet().rebuild()
        // 2. agg functions
        for (aggCall in aggregate.getAggCallList()) {
            inputFieldsUsed.addAll(aggCall.getArgList())
            if (aggCall.filterArg >= 0) {
                inputFieldsUsed.set(aggCall.filterArg)
            }
            if (aggCall.distinctKeys != null) {
                inputFieldsUsed.addAll(aggCall.distinctKeys)
            }
            inputFieldsUsed.addAll(RelCollations.ordinals(aggCall.collation))
        }

        // Create input with trimmed columns.
        val input: RelNode = aggregate.getInput()
        val inputExtraFields: Set<RelDataTypeField> = Collections.emptySet()
        val trimResult = trimChild(aggregate, input, inputFieldsUsed.build(), inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right
        // We have to return group keys and (if present) indicators.
        // So, pretend that the consumer asked for them.
        val groupCount: Int = aggregate.getGroupSet().cardinality()
        fieldsUsed = fieldsUsed.union(ImmutableBitSet.range(groupCount))

        // If the input is unchanged, and we need to project all columns,
        // there's nothing to do.
        if (input === newInput
            && fieldsUsed.equals(ImmutableBitSet.range(rowType.getFieldCount()))
        ) {
            return result(
                aggregate,
                Mappings.createIdentity(rowType.getFieldCount())
            )
        }

        // Which agg calls are used by our consumer?
        var j = groupCount
        var usedAggCallCount = 0
        for (i in 0 until aggregate.getAggCallList().size()) {
            if (fieldsUsed.get(j++)) {
                ++usedAggCallCount
            }
        }

        // Offset due to the number of system fields having changed.
        var mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            rowType.getFieldCount(),
            groupCount + usedAggCallCount
        )
        val newGroupSet: ImmutableBitSet = Mappings.apply(inputMapping, aggregate.getGroupSet())
        val newGroupSets: ImmutableList<ImmutableBitSet> = ImmutableList.copyOf(
            Util.transform(
                aggregate.getGroupSets()
            ) { input1 -> Mappings.apply(inputMapping, input1) })

        // Populate mapping of where to find the fields. System, group key and
        // indicator fields first.
        j = 0
        while (j < groupCount) {
            mapping.set(j, j)
            j++
        }

        // Now create new agg calls, and populate mapping for them.
        relBuilder.push(newInput)
        val newAggCallList: List<RelBuilder.AggCall> = ArrayList()
        j = groupCount
        for (aggCall in aggregate.getAggCallList()) {
            if (fieldsUsed.get(j)) {
                mapping.set(j, groupCount + newAggCallList.size())
                newAggCallList.add(relBuilder.aggregateCall(aggCall, inputMapping))
            }
            ++j
        }
        if (newAggCallList.isEmpty() && newGroupSet.isEmpty()) {
            // Add a dummy call if all the column fields have been trimmed
            mapping = Mappings.create(
                MappingType.INVERSE_SURJECTION,
                mapping.getSourceCount(),
                1
            )
            newAggCallList.add(relBuilder.count(false, "DUMMY"))
        }
        val groupKey: RelBuilder.GroupKey = relBuilder.groupKey(newGroupSet, newGroupSets)
        relBuilder.aggregate(groupKey, newAggCallList)
        val newAggregate: RelNode = RelOptUtil.propagateRelHints(aggregate, relBuilder.build())
        return result(newAggregate, mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalTableModify].
     */
    fun trimFields(
        modifier: LogicalTableModify,
        fieldsUsed: ImmutableBitSet?,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        // Ignore what consumer wants. We always project all columns.
        Util.discard(fieldsUsed)
        val rowType: RelDataType = modifier.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val input: RelNode = modifier.getInput()

        // We want all fields from the child.
        val inputFieldCount: Int = input.getRowType().getFieldCount()
        val inputFieldsUsed: ImmutableBitSet = ImmutableBitSet.range(inputFieldCount)

        // Create input with trimmed columns.
        val inputExtraFields: Set<RelDataTypeField> = Collections.emptySet()
        val trimResult = trimChild(modifier, input, inputFieldsUsed, inputExtraFields)
        val newInput: RelNode = trimResult.left
        val inputMapping: Mapping = trimResult.right
        if (!inputMapping.isIdentity()) {
            // We asked for all fields. Can't believe that the child decided
            // to permute them!
            throw AssertionError(
                "Expected identity mapping, got $inputMapping"
            )
        }
        var newModifier: LogicalTableModify = modifier
        if (newInput !== input) {
            newModifier = modifier.copy(
                modifier.getTraitSet(),
                Collections.singletonList(newInput)
            )
        }
        assert(newModifier.getClass() === modifier.getClass())

        // Always project all fields.
        val mapping: Mapping = Mappings.createIdentity(fieldCount)
        return result(newModifier, mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalTableFunctionScan].
     */
    fun trimFields(
        tabFun: LogicalTableFunctionScan,
        fieldsUsed: ImmutableBitSet?,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        val rowType: RelDataType = tabFun.getRowType()
        val fieldCount: Int = rowType.getFieldCount()
        val newInputs: List<RelNode> = ArrayList()
        for (input in tabFun.getInputs()) {
            val inputFieldCount: Int = input.getRowType().getFieldCount()
            val inputFieldsUsed: ImmutableBitSet = ImmutableBitSet.range(inputFieldCount)

            // Create input with trimmed columns.
            val inputExtraFields: Set<RelDataTypeField> = Collections.emptySet()
            val trimResult = trimChildRestore(
                tabFun, input, inputFieldsUsed, inputExtraFields
            )
            assert(trimResult.right.isIdentity())
            newInputs.add(trimResult.left)
        }
        var newTabFun: LogicalTableFunctionScan = tabFun
        if (!tabFun.getInputs().equals(newInputs)) {
            newTabFun = tabFun.copy(
                tabFun.getTraitSet(), newInputs,
                tabFun.getCall(), tabFun.getElementType(), tabFun.getRowType(),
                tabFun.getColumnMappings()
            )
        }
        assert(newTabFun.getClass() === tabFun.getClass())

        // Always project all fields.
        val mapping: Mapping = Mappings.createIdentity(fieldCount)
        return result(newTabFun, mapping)
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalValues].
     */
    fun trimFields(
        values: LogicalValues,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>?
    ): TrimResult {
        var fieldsUsed: ImmutableBitSet = fieldsUsed
        val rowType: RelDataType = values.getRowType()
        val fieldCount: Int = rowType.getFieldCount()

        // If they are asking for no fields, we can't give them what they want,
        // because zero-column records are illegal. Give them the last field,
        // which is unlikely to be a system field.
        if (fieldsUsed.isEmpty()) {
            fieldsUsed = ImmutableBitSet.range(fieldCount - 1, fieldCount)
        }

        // If all fields are used, return unchanged.
        if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))) {
            val mapping: Mapping = Mappings.createIdentity(fieldCount)
            return result(values, mapping)
        }
        val newTuples: ImmutableList.Builder<ImmutableList<RexLiteral>> = ImmutableList.builder()
        for (tuple in values.getTuples()) {
            val newTuple: ImmutableList.Builder<RexLiteral> = ImmutableList.builder()
            for (field in fieldsUsed) {
                newTuple.add(tuple.get(field))
            }
            newTuples.add(newTuple.build())
        }
        val mapping: Mapping = createMapping(fieldsUsed, fieldCount)
        val newRowType: RelDataType = RelOptUtil.permute(
            values.getCluster().getTypeFactory(), rowType,
            mapping
        )
        val newValues: LogicalValues = LogicalValues.create(
            values.getCluster(), newRowType,
            newTuples.build()
        )
        return result(newValues, mapping)
    }

    protected fun createMapping(fieldsUsed: ImmutableBitSet, fieldCount: Int): Mapping {
        val mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality()
        )
        var i = 0
        for (field in fieldsUsed) {
            mapping.set(field, i++)
        }
        return mapping
    }

    /**
     * Variant of [.trimFields] for
     * [org.apache.calcite.rel.logical.LogicalTableScan].
     */
    fun trimFields(
        tableAccessRel: TableScan,
        fieldsUsed: ImmutableBitSet,
        extraFields: Set<RelDataTypeField?>
    ): TrimResult {
        val fieldCount: Int = tableAccessRel.getRowType().getFieldCount()
        if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
            && extraFields.isEmpty()
        ) {
            // if there is nothing to project or if we are projecting everything
            // then no need to introduce another RelNode
            return trimFields(
                tableAccessRel as RelNode, fieldsUsed, extraFields
            )
        }
        val newTableAccessRel: RelNode = tableAccessRel.project(fieldsUsed, extraFields, relBuilder)

        // Some parts of the system can't handle rows with zero fields, so
        // pretend that one field is used.
        if (fieldsUsed.cardinality() === 0) {
            var input: RelNode = newTableAccessRel
            if (input is Project) {
                // The table has implemented the project in the obvious way - by
                // creating project with 0 fields. Strip it away, and create our own
                // project with one field.
                val project: Project = input as Project
                if (project.getRowType().getFieldCount() === 0) {
                    input = project.getInput()
                }
            }
            return dummyProject(fieldCount, input)
        }
        val mapping: Mapping = createMapping(fieldsUsed, fieldCount)
        return result(newTableAccessRel, mapping)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Result of an attempt to trim columns from a relational expression.
     *
     *
     * The mapping describes where to find the columns wanted by the parent
     * of the current relational expression.
     *
     *
     * The mapping is a
     * [org.apache.calcite.util.mapping.Mappings.SourceMapping], which means
     * that no column can be used more than once, and some columns are not used.
     * `columnsUsed.getSource(i)` returns the source of the i'th output
     * field.
     *
     *
     * For example, consider the mapping for a relational expression that
     * has 4 output columns but only two are being used. The mapping
     * {2  1, 3  0} would give the following behavior:
     *
     *
     *  * columnsUsed.getSourceCount() returns 4
     *  * columnsUsed.getTargetCount() returns 2
     *  * columnsUsed.getSource(0) returns 3
     *  * columnsUsed.getSource(1) returns 2
     *  * columnsUsed.getSource(2) throws IndexOutOfBounds
     *  * columnsUsed.getTargetOpt(3) returns 0
     *  * columnsUsed.getTargetOpt(0) returns -1
     *
     */
    protected class TrimResult(left: RelNode, right: Mapping) : Pair<RelNode?, Mapping?>(left, right) {
        /**
         * Creates a TrimResult.
         *
         * @param left  New relational expression
         * @param right Mapping of fields onto original fields
         */
        init {
            assert(right.getTargetCount() === left.getRowType().getFieldCount()) {
                "rowType: " + left.getRowType().toString() + ", mapping: " + right
            }
        }
    }
}
