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

// TODO jvs 10-Feb-2005:  factor out generic rewrite helper, with the
// ability to map between old and new rels and field ordinals.  Also,
// for now need to prohibit queries which return UDT instances.
/**
 * RelStructuredTypeFlattener removes all structured types from a tree of
 * relational expressions. Because it must operate globally on the tree, it is
 * implemented as an explicit self-contained rewrite operation instead of via
 * normal optimizer rules. This approach has the benefit that real optimizer and
 * codegen rules never have to deal with structured types.
 *
 *
 * As an example, suppose we have a structured type `ST(A1 smallint, A2
 * bigint)`, a table `T(c1 ST, c2 double)`, and a query `
 * select t.c2, t.c1.a2 from t`. After SqlToRelConverter executes, the
 * unflattened tree looks like:
 *
 * <blockquote><pre>`
 * LogicalProject(C2=[$1], A2=[$0.A2])
 * LogicalTableScan(table=[T])
`</pre></blockquote> *
 *
 *
 * After flattening, the resulting tree looks like
 *
 * <blockquote><pre>`
 * LogicalProject(C2=[$3], A2=[$2])
 * FtrsIndexScanRel(table=[T], index=[clustered])
`</pre></blockquote> *
 *
 *
 * The index scan produces a flattened row type `(boolean, smallint,
 * bigint, double)` (the boolean is a null indicator for c1), and the
 * projection picks out the desired attributes (omitting `$0` and
 * `$1` altogether). After optimization, the projection might be
 * pushed down into the index scan, resulting in a final tree like
 *
 * <blockquote><pre>`
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[3, 2])
`</pre></blockquote> *
 */
class RelStructuredTypeFlattener(
    relBuilder: RelBuilder,
    rexBuilder: RexBuilder,
    toRelContext: RelOptTable.ToRelContext,
    restructure: Boolean
) : ReflectiveVisitor {
    //~ Instance fields --------------------------------------------------------
    private val relBuilder: RelBuilder
    private val rexBuilder: RexBuilder
    private val restructure: Boolean
    private val oldToNewRelMap: Map<RelNode, RelNode> = HashMap()

    @Nullable
    private var currentRel: RelNode? = null
    private var iRestructureInput = 0

    @SuppressWarnings("unused")
    @Nullable
    private var flattenedRootType: RelDataType? = null
    var restructured = false
    private val toRelContext: RelOptTable.ToRelContext

    //~ Constructors -----------------------------------------------------------
    @Deprecated // to be removed before 2.0
    constructor(
        rexBuilder: RexBuilder,
        toRelContext: RelOptTable.ToRelContext,
        restructure: Boolean
    ) : this(
        RelFactories.LOGICAL_BUILDER.create(toRelContext.getCluster(), null),
        rexBuilder, toRelContext, restructure
    ) {
    }

    init {
        this.relBuilder = relBuilder
        this.rexBuilder = rexBuilder
        this.toRelContext = toRelContext
        this.restructure = restructure
    }

    //~ Methods ----------------------------------------------------------------
    private val currentRelOrThrow: RelNode
        private get() = requireNonNull(currentRel, "currentRel")

    fun updateRelInMap(
        mapRefRelToCorVar: SortedSetMultimap<RelNode?, CorrelationId?>
    ) {
        for (rel in Lists.newArrayList(mapRefRelToCorVar.keySet())) {
            if (oldToNewRelMap.containsKey(rel)) {
                val corVarSet: SortedSet<CorrelationId> = mapRefRelToCorVar.removeAll(rel)
                mapRefRelToCorVar.putAll(oldToNewRelMap[rel], corVarSet)
            }
        }
    }

    @SuppressWarnings(["JdkObsolete", "ModifyCollectionInEnhancedForLoop"])
    fun updateRelInMap(
        mapCorVarToCorRel: SortedMap<CorrelationId?, LogicalCorrelate?>
    ) {
        for (corVar in mapCorVarToCorRel.keySet()) {
            val oldRel: LogicalCorrelate = mapCorVarToCorRel.get(corVar)
            if (oldToNewRelMap.containsKey(oldRel)) {
                val newRel: RelNode? = oldToNewRelMap[oldRel]
                assert(newRel is LogicalCorrelate)
                mapCorVarToCorRel.put(corVar, newRel as LogicalCorrelate?)
            }
        }
    }

    fun rewrite(root: RelNode): RelNode {
        // Perform flattening.
        val visitor: RewriteRelVisitor = RewriteRelVisitor()
        visitor.visit(root, 0, null)
        val flattened: RelNode = getNewForOldRel(root)
        flattenedRootType = flattened.getRowType()
        return if (restructure) {
            tryRestructure(root, flattened)
        } else flattened
    }

    private fun tryRestructure(root: RelNode, flattened: RelNode): RelNode {
        iRestructureInput = 0
        restructured = false
        val structuringExps: List<RexNode> = restructureFields(root.getRowType())
        return if (restructured) {
            val resultFieldNames: List<String> = root.getRowType().getFieldNames()
            // If requested, add an additional projection which puts
            // everything back into structured form for return to the
            // client.
            var restructured: RelNode = relBuilder.push(flattened)
                .projectNamed(structuringExps, resultFieldNames, true)
                .build()
            restructured = RelOptUtil.copyRelHints(flattened, restructured)
            // REVIEW jvs 23-Mar-2005:  How do we make sure that this
            // implementation stays in Java?  Fennel can't handle
            // structured types.
            restructured
        } else {
            flattened
        }
    }

    /**
     * When called with old root rowType it's known that flattened root (which may become input)
     * returns flat fields, so it simply refers flat fields by increasing index and collects them
     * back into struct constructor expressions if necessary.
     *
     * @param structuredType old root rowType or it's nested struct
     * @return list of rex nodes, some of them may collect flattened struct's fields back
     * into original structure to return correct type for client
     */
    private fun restructureFields(structuredType: RelDataType): List<RexNode> {
        val structuringExps: List<RexNode> = ArrayList()
        for (field in structuredType.getFieldList()) {
            val fieldType: RelDataType = field.getType()
            var expr: RexNode?
            if (fieldType.isStruct()) {
                restructured = true
                expr = restructure(fieldType)
            } else {
                expr = RexInputRef(iRestructureInput++, fieldType)
            }
            structuringExps.add(expr)
        }
        return structuringExps
    }

    private fun restructure(structuredType: RelDataType): RexNode {
        // Use ROW(f1,f2,...,fn) to put flattened data back together into a structure.
        val structFields: List<RexNode> = restructureFields(structuredType)
        return rexBuilder.makeCall(
            structuredType,
            SqlStdOperatorTable.ROW,
            structFields
        )
    }

    protected fun setNewForOldRel(oldRel: RelNode?, newRel: RelNode?) {
        oldToNewRelMap.put(oldRel, newRel)
    }

    protected fun getNewForOldRel(oldRel: RelNode): RelNode {
        return requireNonNull(
            oldToNewRelMap[oldRel]
        ) { "newRel not found for $oldRel" }
    }

    /**
     * Maps the ordinal of a field pre-flattening to the ordinal of the
     * corresponding field post-flattening.
     *
     * @param oldOrdinal Pre-flattening ordinal
     * @return Post-flattening ordinal
     */
    protected fun getNewForOldInput(oldOrdinal: Int): Int {
        return getNewFieldForOldInput(oldOrdinal).i
    }

    /**
     * Finds type and new ordinal relative to new inputs by oldOrdinal and
     * innerOrdinal indexes.
     *
     * @param oldOrdinal   ordinal of the field relative to old inputs
     * @param innerOrdinal when oldOrdinal points to struct and target field
     * is inner field of struct, this argument should contain
     * calculated field's ordinal within struct after flattening.
     * Otherwise when oldOrdinal points to primitive field, this
     * argument should be zero.
     * @return flat type with new ordinal relative to new inputs
     */
    private fun getNewFieldForOldInput(oldOrdinal: Int, innerOrdinal: Int): Ord<RelDataType> {
        // sum of predecessors post flatten sizes points to new ordinal
        // of flat field or first field of flattened struct
        val postFlatteningOrdinal: Int = currentRelOrThrow.getInputs().stream()
            .flatMap { node -> node.getRowType().getFieldList().stream() }
            .limit(oldOrdinal)
            .map(RelDataTypeField::getType)
            .mapToInt { type: RelDataType -> postFlattenSize(type) }
            .sum()
        val newOrdinal = postFlatteningOrdinal + innerOrdinal
        // NoSuchElementException may be thrown because of two reasons:
        // 1. postFlattenSize() didn't predict well
        // 2. innerOrdinal has wrong value
        val newField: RelDataTypeField = getNewInputFieldByNewOrdinal(newOrdinal)
        return Ord.of(newOrdinal, newField.getType())
    }

    private fun getNewInputFieldByNewOrdinal(newOrdinal: Int): RelDataTypeField {
        return currentRelOrThrow.getInputs().stream()
            .map { oldRel: RelNode -> getNewForOldRel(oldRel) }
            .flatMap { node -> node.getRowType().getFieldList().stream() }
            .skip(newOrdinal)
            .findFirst()
            .orElseThrow { NoSuchElementException() }
    }

    /** Returns whether the old field at index `fieldIdx` was not flattened.  */
    private fun noFlatteningForInput(fieldIdx: Int): Boolean {
        val inputs: List<RelNode> = currentRelOrThrow.getInputs()
        var fieldCnt = 0
        for (input in inputs) {
            fieldCnt += input.getRowType().getFieldCount()
            if (fieldCnt > fieldIdx) {
                return (getNewForOldRel(input).getRowType().getFieldList().size()
                        === input.getRowType().getFieldList().size())
            }
        }
        return false
    }

    /**
     * Maps the ordinal of a field pre-flattening to the ordinal of the
     * corresponding field post-flattening, and also returns its type.
     *
     * @param oldOrdinal Pre-flattening ordinal
     * @return Post-flattening ordinal and type
     */
    protected fun getNewFieldForOldInput(oldOrdinal: Int): Ord<RelDataType> {
        return getNewFieldForOldInput(oldOrdinal, 0)
    }

    /**
     * Returns a mapping between old and new fields.
     *
     * @param oldRel Old relational expression
     * @return Mapping between fields of old and new
     */
    private fun getNewForOldInputMapping(oldRel: RelNode): Mappings.TargetMapping {
        val newRel: RelNode = getNewForOldRel(oldRel)
        return Mappings.target(
            { oldOrdinal: Int -> getNewForOldInput(oldOrdinal) },
            oldRel.getRowType().getFieldCount(),
            newRel.getRowType().getFieldCount()
        )
    }

    private fun getPostFlatteningOrdinal(preFlattenRowType: RelDataType, preFlattenOrdinal: Int): Int {
        return preFlattenRowType.getFieldList().stream()
            .limit(preFlattenOrdinal)
            .map(RelDataTypeField::getType)
            .mapToInt { type: RelDataType -> postFlattenSize(type) }
            .sum()
    }

    private fun postFlattenSize(type: RelDataType): Int {
        return if (type.isStruct()) {
            type.getFieldList().stream()
                .map(RelDataTypeField::getType)
                .mapToInt { type: RelDataType -> postFlattenSize(type) }
                .sum()
        } else {
            1
        }
    }

    fun rewriteRel(rel: LogicalTableModify) {
        val newRel: LogicalTableModify = LogicalTableModify.create(
            rel.getTable(),
            rel.getCatalogReader(),
            getNewForOldRel(rel.getInput()),
            rel.getOperation(),
            rel.getUpdateColumnList(),
            rel.getSourceExpressionList(),
            true
        )
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: LogicalAggregate) {
        val oldInput: RelNode = rel.getInput()
        val inputType: RelDataType = oldInput.getRowType()
        val inputTypeFields: List<RelDataTypeField> = inputType.getFieldList()
        if (SqlTypeUtil.isFlat(inputType) || rel.getAggCallList().stream().allMatch { call ->
                (call.getArgList().isEmpty()
                        || call.getArgList().stream().noneMatch { idx ->
                    inputTypeFields[idx]
                        .getType().isStruct()
                })
            }) {
            rewriteGeneric(rel)
        } else {
            // one of aggregate calls definitely refers to field with struct type from oldInput,
            // let's restructure new input back and use restructured one as new input for aggregate node
            val restructuredInput: RelNode = tryRestructure(oldInput, getNewForOldRel(oldInput))
            // expected that after restructuring indexes in AggregateCalls again became relevant,
            // leave it as is but with new input
            var newRel: RelNode = rel.copy(
                rel.getTraitSet(), restructuredInput, rel.getGroupSet(),
                rel.getGroupSets(), rel.getAggCallList()
            )
            if (!SqlTypeUtil.isFlat(rel.getRowType())) {
                newRel = coverNewRelByFlatteningProjection(rel, newRel)
            }
            setNewForOldRel(rel, newRel)
        }
    }

    fun rewriteRel(rel: Sort) {
        val oldCollation: RelCollation = rel.getCollation()
        val oldChild: RelNode = rel.getInput()
        val newChild: RelNode = getNewForOldRel(oldChild)
        val mapping: Mappings.TargetMapping = getNewForOldInputMapping(oldChild)

        // validate
        for (field in oldCollation.getFieldCollations()) {
            val oldInput: Int = field.getFieldIndex()
            val sortFieldType: RelDataType = oldChild.getRowType().getFieldList().get(oldInput).getType()
            if (sortFieldType.isStruct()) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement("sorting on structured types")
            }
        }
        val newCollation: RelCollation = RexUtil.apply(mapping, oldCollation)
        val newRel: Sort = LogicalSort.create(newChild, newCollation, rel.offset, rel.fetch)
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: LogicalFilter) {
        val traits: RelTraitSet = rel.getTraitSet()
        val rewriteRexShuttle: RewriteRexShuttle = RewriteRexShuttle()
        val oldCondition: RexNode = rel.getCondition()
        val newInput: RelNode = getNewForOldRel(rel.getInput())
        val newCondition: RexNode = oldCondition.accept(rewriteRexShuttle)
        val newRel: LogicalFilter = rel.copy(traits, newInput, newCondition)
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: LogicalJoin) {
        val newRel: LogicalJoin = LogicalJoin.create(
            getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            rel.getHints(),
            rel.getCondition().accept(RewriteRexShuttle()),
            rel.getVariablesSet(), rel.getJoinType()
        )
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: LogicalCorrelate) {
        val newPos: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (pos in rel.getRequiredColumns()) {
            val corrFieldType: RelDataType = rel.getLeft().getRowType().getFieldList().get(pos)
                .getType()
            if (corrFieldType.isStruct()) {
                throw Util.needToImplement("correlation on structured type")
            }
            newPos.set(getNewForOldInput(pos))
        }
        val newRel: LogicalCorrelate = LogicalCorrelate.create(
            getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            rel.getCorrelationId(),
            newPos.build(),
            rel.getJoinType()
        )
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: Collect) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: Uncollect) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalIntersect) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalMinus) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalUnion) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalValues) {
        // NOTE jvs 30-Apr-2006:  UDT instances require invocation
        // of a constructor method, which can't be represented
        // by the tuples stored in a LogicalValues, so we don't have
        // to worry about them here.
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalTableFunctionScan) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: Sample) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalProject) {
        val shuttle: RewriteRexShuttle = RewriteRexShuttle()
        val oldProjects: List<RexNode> = rel.getProjects()
        val oldNames: List<String> = rel.getRowType().getFieldNames()
        val flattenedExpList: List<Pair<RexNode, String>> = ArrayList()
        flattenProjections(shuttle, oldProjects, oldNames, "", flattenedExpList)
        val newInput: RelNode = getNewForOldRel(rel.getInput())
        val newProjects: List<RexNode> = Pair.left(flattenedExpList)
        val newNames: List<String> = Pair.right(flattenedExpList)
        val newRel: RelNode = relBuilder.push(newInput)
            .projectNamed(newProjects, newNames, true)
            .hints(rel.getHints())
            .build()
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: LogicalCalc) {
        // Translate the child.
        val newInput: RelNode = getNewForOldRel(rel.getInput())
        val cluster: RelOptCluster = rel.getCluster()
        val programBuilder = RexProgramBuilder(
            newInput.getRowType(),
            cluster.getRexBuilder()
        )

        // Convert the common expressions.
        val program: RexProgram = rel.getProgram()
        val shuttle: RewriteRexShuttle = RewriteRexShuttle()
        for (expr in program.getExprList()) {
            programBuilder.registerInput(expr.accept(shuttle))
        }

        // Convert the projections.
        val flattenedExpList: List<Pair<RexNode, String>> = ArrayList()
        val fieldNames: List<String> = rel.getRowType().getFieldNames()
        flattenProjections(
            RewriteRexShuttle(),
            program.getProjectList(),
            fieldNames,
            "",
            flattenedExpList
        )

        // Register each of the new projections.
        for (flattenedExp in flattenedExpList) {
            programBuilder.addProject(flattenedExp.left, flattenedExp.right)
        }

        // Translate the condition.
        val conditionRef: RexLocalRef = program.getCondition()
        if (conditionRef != null) {
            val newField: Ord<RelDataType> = getNewFieldForOldInput(conditionRef.getIndex())
            programBuilder.addCondition(RexLocalRef(newField.i, newField.e))
        }
        val newProgram: RexProgram = programBuilder.getProgram()

        // Create a new calc relational expression.
        val newRel: LogicalCalc = LogicalCalc.create(newInput, newProgram)
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: SelfFlatteningRel) {
        rel.flattenRel(this)
    }

    fun rewriteGeneric(rel: RelNode) {
        val newRel: RelNode = rel.copy(rel.getTraitSet(), rel.getInputs())
        val oldInputs: List<RelNode> = rel.getInputs()
        for (i in 0 until oldInputs.size()) {
            newRel.replaceInput(
                i,
                getNewForOldRel(oldInputs[i])
            )
        }
        setNewForOldRel(rel, newRel)
    }

    private fun flattenProjections(
        shuttle: RewriteRexShuttle,
        exps: List<RexNode?>,
        @Nullable fieldNames: List<String?>?,
        prefix: String,
        flattenedExps: List<Pair<RexNode, String>>
    ) {
        for (i in 0 until exps.size()) {
            val exp: RexNode? = exps[i]
            val fieldName = extractName(fieldNames, prefix, i)
            flattenProjection(shuttle, exp, fieldName, flattenedExps)
        }
    }

    private fun flattenProjection(
        shuttle: RewriteRexShuttle,
        exp: RexNode?,
        fieldName: String,
        flattenedExps: List<Pair<RexNode, String>>
    ) {
        if (exp.getType().isStruct()) {
            if (exp is RexInputRef) {
                val oldOrdinal: Int = (exp as RexInputRef?).getIndex()
                val flattenFieldsCount = postFlattenSize(exp.getType())
                for (innerOrdinal in 0 until flattenFieldsCount) {
                    val newField: Ord<RelDataType> = getNewFieldForOldInput(oldOrdinal, innerOrdinal)
                    val newRef = RexInputRef(newField.i, newField.e)
                    flattenedExps.add(Pair.of(newRef, fieldName))
                }
            } else if (isConstructor(exp) || exp.isA(SqlKind.CAST)) {
                // REVIEW jvs 27-Feb-2005:  for cast, see corresponding note
                // in RewriteRexShuttle
                val call: RexCall? = exp as RexCall?
                if (exp.isA(SqlKind.CAST)
                    && RexLiteral.isNullLiteral(call.operands.get(0))
                ) {
                    // Translate CAST(NULL AS UDT) into
                    // the correct number of null fields.
                    flattenNullLiteral(exp.getType(), flattenedExps)
                    return
                }
                flattenProjections(
                    shuttle,
                    call.getOperands(),
                    Collections.nCopies(call.getOperands().size(), null),
                    fieldName,
                    flattenedExps
                )
            } else if (exp is RexCall) {
                // NOTE jvs 10-Feb-2005:  This is a lame hack to keep special
                // functions which return row types working.
                var newExp: RexNode? = exp
                val operands: List<RexNode> = (exp as RexCall?).getOperands()
                val operator: SqlOperator = (exp as RexCall?).getOperator()
                if (operator === SqlStdOperatorTable.ITEM && operands[0].getType().isStruct()
                    && operands[1].isA(SqlKind.LITERAL)
                    && SqlTypeUtil.inCharFamily(operands[1].getType())
                ) {
                    val literalString: String = (operands[1] as RexLiteral).getValueAs(String::class.java)
                    val firstOp: RexNode = operands[0]
                    if (firstOp is RexInputRef) {
                        // when performed getting field from struct exp by field name
                        // and new input is flattened it's enough to refer target field by index.
                        // But it's possible that requested field is also of type struct, that's
                        // why we're trying to get range from to. For primitive just one field will be in range.
                        var from = 0
                        for (field in firstOp.getType().getFieldList()) {
                            from += if (field.getName().equalsIgnoreCase(literalString)) {
                                val oldOrdinal: Int = (firstOp as RexInputRef).getIndex()
                                val to = from + postFlattenSize(field.getType())
                                for (newInnerOrdinal in from until to) {
                                    val newField: Ord<RelDataType> = getNewFieldForOldInput(oldOrdinal, newInnerOrdinal)
                                    val newRef: RexInputRef = rexBuilder.makeInputRef(newField.e, newField.i)
                                    flattenedExps.add(Pair.of(newRef, fieldName))
                                }
                                break
                            } else {
                                postFlattenSize(field.getType())
                            }
                        }
                    } else if (firstOp is RexCall) {
                        // to get nested struct from return type of firstOp rex call,
                        // we need to flatten firstOp and get range of expressions which
                        // corresponding to desirable nested struct flattened fields
                        val firstOpFlattenedExps: List<Pair<RexNode, String>> = ArrayList()
                        flattenProjection(shuttle, firstOp, "$fieldName$0", firstOpFlattenedExps)
                        val newInnerOrdinal = getNewInnerOrdinal(firstOp, literalString)
                        val endOfRange = newInnerOrdinal + postFlattenSize(newExp.getType())
                        for (i in newInnerOrdinal until endOfRange) {
                            flattenedExps.add(firstOpFlattenedExps[i])
                        }
                    }
                } else {
                    newExp = rexBuilder.makeCall(
                        exp.getType(), operator,
                        shuttle.visitList(operands)
                    )
                    // flatten call result type
                    flattenResultTypeOfRexCall(newExp, fieldName, flattenedExps)
                }
            } else {
                throw Util.needToImplement(exp)
            }
        } else {
            flattenedExps.add(
                Pair.of(exp.accept(shuttle), fieldName)
            )
        }
    }

    private fun flattenResultTypeOfRexCall(
        newExp: RexNode?,
        fieldName: String,
        flattenedExps: List<Pair<RexNode, String>>
    ) {
        var nameIdx = 0
        for (field in newExp.getType().getFieldList()) {
            val fieldRef: RexNode = rexBuilder.makeFieldAccess(newExp, field.getIndex())
            val fieldRefName = fieldName + "$" + nameIdx++
            if (fieldRef.getType().isStruct()) {
                flattenResultTypeOfRexCall(fieldRef, fieldRefName, flattenedExps)
            } else {
                flattenedExps.add(Pair.of(fieldRef, fieldRefName))
            }
        }
    }

    private fun flattenNullLiteral(
        type: RelDataType,
        flattenedExps: List<Pair<RexNode, String>>
    ) {
        val flattenedType: RelDataType = SqlTypeUtil.flattenRecordType(rexBuilder.getTypeFactory(), type, null)
        for (field in flattenedType.getFieldList()) {
            flattenedExps.add(
                Pair.of(
                    rexBuilder.makeNullLiteral(field.getType()),
                    field.getName()
                )
            )
        }
    }

    fun rewriteRel(rel: TableScan) {
        var newRel: RelNode = rel.getTable().toRel(toRelContext)
        newRel = if (!SqlTypeUtil.isFlat(rel.getRowType())) {
            coverNewRelByFlatteningProjection(rel, newRel)
        } else {
            RelOptUtil.copyRelHints(rel, newRel)
        }
        setNewForOldRel(rel, newRel)
    }

    private fun coverNewRelByFlatteningProjection(rel: RelNode, newRel: RelNode): RelNode {
        var newRel: RelNode = newRel
        val flattenedExpList: List<Pair<RexNode, String>> = ArrayList()
        val newRowRef: RexNode = rexBuilder.makeRangeReference(newRel)
        val inputRowFields: List<RelDataTypeField> = rel.getRowType().getFieldList()
        flattenInputs(inputRowFields, newRowRef, flattenedExpList)
        // cover new scan with flattening projection
        val projects: List<RexNode> = Pair.left(flattenedExpList)
        val fieldNames: List<String> = Pair.right(flattenedExpList)
        newRel = relBuilder.push(newRel)
            .projectNamed(projects, fieldNames, true)
            .build()
        newRel = RelOptUtil.copyRelHints(rel, newRel)
        return newRel
    }

    fun rewriteRel(rel: LogicalSnapshot) {
        val newRel: RelNode = rel.copy(
            rel.getTraitSet(),
            getNewForOldRel(rel.getInput()),
            rel.getPeriod().accept(RewriteRexShuttle())
        )
        setNewForOldRel(rel, newRel)
    }

    fun rewriteRel(rel: LogicalDelta) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalChi) {
        rewriteGeneric(rel)
    }

    fun rewriteRel(rel: LogicalMatch) {
        rewriteGeneric(rel)
    }

    /** Generates expressions that reference the flattened input fields from
     * a given row type.  */
    private fun flattenInputs(
        fieldList: List<RelDataTypeField>, prefix: RexNode,
        flattenedExpList: List<Pair<RexNode, String>>
    ) {
        for (field in fieldList) {
            val ref: RexNode = rexBuilder.makeFieldAccess(prefix, field.getIndex())
            if (field.getType().isStruct()) {
                val structFields: List<RelDataTypeField> = field.getType().getFieldList()
                flattenInputs(structFields, ref, flattenedExpList)
            } else {
                flattenedExpList.add(Pair.of(ref, field.getName()))
            }
        }
    }
    //~ Inner Interfaces -------------------------------------------------------
    /** Mix-in interface for relational expressions that know how to
     * flatten themselves.  */
    interface SelfFlatteningRel : RelNode {
        fun flattenRel(flattener: RelStructuredTypeFlattener?)
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Visitor that flattens each relational expression in a tree.  */
    private inner class RewriteRelVisitor : RelVisitor() {
        private val dispatcher: ReflectiveVisitDispatcher<RelStructuredTypeFlattener, RelNode> =
            ReflectUtil.createDispatcher(
                RelStructuredTypeFlattener::class.java,
                RelNode::class.java
            )

        @Override
        fun visit(p: RelNode, ordinal: Int, @Nullable parent: RelNode?) {
            // rewrite children first
            super.visit(p, ordinal, parent)
            currentRel = p
            val visitMethodName = "rewriteRel"
            val found: Boolean = dispatcher.invokeVisitor(
                this@RelStructuredTypeFlattener,
                currentRel,
                visitMethodName
            )
            currentRel = null
            if (!found) {
                if (p.getInputs().size() === 0) {
                    // for leaves, it's usually safe to assume that
                    // no transformation is required
                    rewriteGeneric(p)
                } else {
                    throw AssertionError(
                        "no '" + visitMethodName
                                + "' method found for class " + p.getClass().getName()
                    )
                }
            }
        }
    }

    /** Shuttle that rewrites scalar expressions.  */
    private inner class RewriteRexShuttle : RexShuttle() {
        @Override
        fun visitInputRef(input: RexInputRef): RexNode {
            val oldIndex: Int = input.getIndex()
            val field: Ord<RelDataType> = getNewFieldForOldInput(oldIndex)
            val inputFieldByOldIndex: RelDataTypeField = currentRelOrThrow.getInputs().stream()
                .flatMap { relInput -> relInput.getRowType().getFieldList().stream() }
                .skip(oldIndex)
                .findFirst()
                .orElseThrow { AssertionError("Found input ref with index not found in old inputs") }
            if (inputFieldByOldIndex.getType().isStruct()) {
                iRestructureInput = field.i
                val rexNodes: List<RexNode> = restructureFields(inputFieldByOldIndex.getType())
                return rexBuilder.makeCall(
                    inputFieldByOldIndex.getType(),
                    SqlStdOperatorTable.ROW,
                    rexNodes
                )
            }

            // Use the actual flattened type, which may be different from the current
            // type.
            val fieldType: RelDataType = removeDistinct(field.e)
            return RexInputRef(field.i, fieldType)
        }

        private fun removeDistinct(type: RelDataType): RelDataType {
            return if (type.getSqlTypeName() !== SqlTypeName.DISTINCT) {
                type
            } else type.getFieldList().get(0).getType()
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode? {
            // walk down the field access path expression, calculating
            // the desired input number
            var fieldAccess: RexFieldAccess = fieldAccess
            var iInput = 0
            val accessOrdinals: Deque<Integer> = ArrayDeque()
            while (true) {
                var refExp: RexNode = fieldAccess.getReferenceExpr()
                val ordinal: Int = fieldAccess.getField().getIndex()
                accessOrdinals.push(ordinal)
                iInput += getPostFlatteningOrdinal(
                    refExp.getType(),
                    ordinal
                )
                if (refExp is RexInputRef) {
                    // Consecutive field accesses over some input can be removed since by now the input
                    // is flattened (no struct types). We just have to create a new RexInputRef with the
                    // correct ordinal and type.
                    val inputRef: RexInputRef = refExp as RexInputRef
                    if (noFlatteningForInput(inputRef.getIndex())) {
                        // Sanity check, the input must not have struct type fields.
                        // We better have a record for each old input field
                        // whether it is flattened.
                        return fieldAccess
                    }
                    val newField: Ord<RelDataType> = getNewFieldForOldInput(inputRef.getIndex(), iInput)
                    return RexInputRef(newField.getKey(), removeDistinct(newField.getValue()))
                } else if (refExp is RexCorrelVariable) {
                    val refType: RelDataType = SqlTypeUtil.flattenRecordType(
                        rexBuilder.getTypeFactory(), refExp.getType(), null
                    )
                    refExp = rexBuilder.makeCorrel(refType, (refExp as RexCorrelVariable).id)
                    return rexBuilder.makeFieldAccess(refExp, iInput)
                } else if (refExp is RexCall) {
                    // Field accesses over calls cannot be simplified since the result of the call may be
                    // a struct type.
                    val call: RexCall = refExp as RexCall
                    var newRefExp: RexNode? = visitCall(call)
                    for (ord in accessOrdinals) {
                        newRefExp = rexBuilder.makeFieldAccess(newRefExp, ord)
                    }
                    return newRefExp
                } else if (refExp is RexFieldAccess) {
                    fieldAccess = refExp as RexFieldAccess
                } else {
                    throw Util.needToImplement(refExp)
                }
            }
        }

        @Override
        fun visitCall(rexCall: RexCall): RexNode? {
            if (rexCall.isA(SqlKind.CAST)) {
                val input: RexNode = rexCall.getOperands().get(0).accept(this)
                val targetType: RelDataType = removeDistinct(rexCall.getType())
                return rexBuilder.makeCast(
                    targetType,
                    input
                )
            }
            if (rexCall.op === SqlStdOperatorTable.ITEM && rexCall.operands.get(0).getType().isStruct()
                && rexCall.operands.get(1).isA(SqlKind.LITERAL)
                && SqlTypeUtil.inCharFamily(rexCall.operands.get(1).getType())
            ) {
                val firstOp: RexNode = rexCall.operands.get(0)
                val literalString: String = (rexCall.operands.get(1) as RexLiteral)
                    .getValueAs(String::class.java)
                if (firstOp is RexInputRef) {
                    val oldOrdinal: Int = (firstOp as RexInputRef).getIndex()
                    val newInnerOrdinal = getNewInnerOrdinal(firstOp, literalString)
                    val newField: Ord<RelDataType> = getNewFieldForOldInput(oldOrdinal, newInnerOrdinal)
                    return rexBuilder.makeInputRef(newField.e, newField.i)
                } else {
                    val newFirstOp: RexNode = firstOp.accept(this)
                    if (newFirstOp is RexInputRef) {
                        val newRefOrdinal: Int = ((newFirstOp as RexInputRef).getIndex()
                                + getNewInnerOrdinal(firstOp, literalString))
                        val newField: RelDataTypeField = getNewInputFieldByNewOrdinal(newRefOrdinal)
                        return rexBuilder.makeInputRef(newField.getType(), newRefOrdinal)
                    }
                }
            }
            if (!rexCall.isA(SqlKind.COMPARISON)) {
                return super.visitCall(rexCall)
            }
            val lhs: RexNode = rexCall.getOperands().get(0)
            return if (!lhs.getType().isStruct()) {
                // NOTE jvs 9-Mar-2005:  Calls like IS NULL operate
                // on the representative null indicator.  Since it comes
                // first, we don't have to do any special translation.
                super.visitCall(rexCall)
            } else flattenComparison(
                rexBuilder,
                rexCall.getOperator(),
                rexCall.getOperands()
            )

            // NOTE jvs 22-Mar-2005:  Likewise, the null indicator takes
            // care of comparison null semantics without any special casing.
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery): RexNode {
            var subQuery: RexSubQuery = subQuery
            subQuery = super.visitSubQuery(subQuery) as RexSubQuery
            val flattener = RelStructuredTypeFlattener(
                relBuilder, rexBuilder,
                toRelContext, restructure
            )
            val rel: RelNode = flattener.rewrite(subQuery.rel)
            return subQuery.clone(rel)
        }

        private fun flattenComparison(
            rexBuilder: RexBuilder,
            op: SqlOperator,
            @MinLen(1) exprs: List<RexNode>
        ): RexNode? {
            var op: SqlOperator = op
            val flattenedExps: List<Pair<RexNode, String>> = ArrayList()
            flattenProjections(this, exprs, null, "", flattenedExps)
            val n: Int = flattenedExps.size() / 2
            if (n == 0) {
                throw IllegalArgumentException("exprs must be non-empty")
            }
            var negate = false
            if (op.getKind() === SqlKind.NOT_EQUALS) {
                negate = true
                op = SqlStdOperatorTable.EQUALS
            }
            if (n > 1 && op.getKind() !== SqlKind.EQUALS) {
                throw Util.needToImplement(
                    "inequality comparison for row types"
                )
            }
            var conjunction: RexNode? = null
            for (i in 0 until n) {
                val comparison: RexNode = rexBuilder.makeCall(
                    op,
                    flattenedExps[i].left,
                    flattenedExps[i + n].left
                )
                conjunction = if (conjunction == null) {
                    comparison
                } else {
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        conjunction,
                        comparison
                    )
                }
            }
            requireNonNull(conjunction, "conjunction must be non-null")
            return if (negate) {
                rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT,
                    conjunction
                )
            } else {
                conjunction
            }
        }
    }

    private fun getNewInnerOrdinal(firstOp: RexNode, @Nullable literalString: String): Int {
        var newInnerOrdinal = 0
        for (field in firstOp.getType().getFieldList()) {
            newInnerOrdinal += if (field.getName().equalsIgnoreCase(literalString)) {
                break
            } else {
                postFlattenSize(field.getType())
            }
        }
        return newInnerOrdinal
    }

    companion object {
        private fun extractName(
            @Nullable fieldNames: List<String?>?,
            prefix: String, i: Int
        ): String {
            var fieldName = if (fieldNames == null || fieldNames[i] == null) "$$i" else fieldNames[i]!!
            if (!prefix.equals("")) {
                fieldName = "$prefix$$fieldName"
            }
            return fieldName
        }

        private fun isConstructor(rexNode: RexNode?): Boolean {
            // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
            if (rexNode !is RexCall) {
                return false
            }
            val call: RexCall? = rexNode as RexCall?
            return (call.getOperator().getName().equalsIgnoreCase("row")
                    || call.isA(SqlKind.NEW_SPECIFICATION))
        }
    }
}
