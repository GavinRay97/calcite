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
package org.apache.calcite.rel

import org.apache.calcite.plan.Convention

/**
 * Base class for every relational expression ([RelNode]).
 */
abstract class AbstractRelNode protected constructor(cluster: RelOptCluster?, traitSet: RelTraitSet?) : RelNode {
    //~ Instance fields --------------------------------------------------------
    /**
     * Cached type of this relational expression.
     */
    @MonotonicNonNull
    protected var rowType: RelDataType? = null

    /**
     * The digest that uniquely identifies the node.
     */
    @API(since = "1.24", status = API.Status.INTERNAL)
    protected var digest: RelDigest
    private val cluster: RelOptCluster?

    /** Unique id of this object, for debugging.  */
    @get:Override
    val id: Int

    /** RelTraitSet that describes the traits of this RelNode.  */
    protected var traitSet: RelTraitSet?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an `AbstractRelNode`.
     */
    init {
        assert(cluster != null)
        this.cluster = cluster
        this.traitSet = traitSet
        id = AbstractRelNode.Companion.NEXT_ID.getAndIncrement()
        digest = AbstractRelNode.InnerRelDigest()
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        // Note that empty set equals empty set, so relational expressions
        // with zero inputs do not generally need to implement their own copy
        // method.
        if (inputs.equals(inputs)
            && traitSet === getTraitSet()
        ) {
            return this
        }
        throw AssertionError(
            "Relational expression should override copy. "
                    + "Class=[" + getClass()
                    + "]; traits=[" + getTraitSet()
                    + "]; desired traits=[" + traitSet
                    + "]"
        )
    }

    @Override
    fun getCluster(): RelOptCluster? {
        return cluster
    }

    @get:Nullable
    @get:Override
    @get:Pure
    val convention: Convention?
        get() = if (traitSet == null) null else traitSet.getTrait(ConventionTraitDef.INSTANCE)

    @Override
    fun getTraitSet(): RelTraitSet? {
        return traitSet
    }

    @get:Nullable
    @get:Override
    val correlVariable: String?
        get() = null

    @Override
    fun getInput(i: Int): RelNode {
        val inputs: List<RelNode> = inputs
        return inputs[i]
    }

    @Override
    fun register(planner: RelOptPlanner?) {
        Util.discard(planner)
    }

    // It is not recommended to override this method, but sub-classes can do it at their own risk.
    @get:Override
    val relTypeName: String?
        get() {
            val cn: String = getClass().getName()
            var i: Int = cn.length()
            while (--i >= 0) {
                if (cn.charAt(i) === '$' || cn.charAt(i) === '.') {
                    return cn.substring(i + 1)
                }
            }
            return cn
        }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        return litmus.succeed()
    }

    @Override
    fun getRowType(): RelDataType? {
        if (rowType == null) {
            rowType = deriveRowType()
            assert(rowType != null) { this }
        }
        return rowType
    }

    protected fun deriveRowType(): RelDataType {
        // This method is only called if rowType is null, so you don't NEED to
        // implement it if rowType is always set.
        throw UnsupportedOperationException()
    }

    @Override
    fun getExpectedInputRowType(ordinalInParent: Int): RelDataType? {
        return getRowType()
    }

    @get:Override
    val inputs: List<org.apache.calcite.rel.RelNode?>?
        get() = Collections.emptyList()

    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return 1.0
    }

    @get:Override
    val variablesSet: Set<Any?>?
        get() = ImmutableSet.of()

    @Override
    fun collectVariablesUsed(variableSet: Set<CorrelationId?>?) {
        // for default case, nothing to do
    }

    @get:Override
    val isEnforcer: Boolean
        get() = false

    @Override
    fun collectVariablesSet(variableSet: Set<CorrelationId?>?) {
    }

    @Override
    fun childrenAccept(visitor: RelVisitor) {
        val inputs: List<RelNode> = inputs
        for (i in 0 until inputs.size()) {
            visitor.visit(inputs[i], i, this)
        }
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        // Call fall-back method. Specific logical types (such as LogicalProject
        // and LogicalJoin) have their own RelShuttle.visit methods.
        return shuttle.visit(this)
    }

    @Override
    fun accept(shuttle: RexShuttle?): RelNode {
        return this
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // by default, assume cost is proportional to number of rows
        val rowCount: Double = mq.getRowCount(this)
        return planner.getCostFactory().makeCost(rowCount, rowCount, 0)
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> metadata(
        metadataClass: Class<M>,
        mq: RelMetadataQuery?
    ): M? {
        val factory: MetadataFactory = cluster.getMetadataFactory()
        val metadata: M = factory.query(this, mq, metadataClass)
        assert(metadata != null) {
            ("no provider found (rel=" + this + ", m=" + metadataClass
                    + "); a backstop provider is recommended")
        }
        // Usually the metadata belongs to the rel that created it. RelSubset and
        // HepRelVertex are notable exceptions, so disable the assert. It's not
        // worth the performance hit to override this method for them.
        //   assert metadata.rel() == this : "someone else's metadata";
        return metadata
    }

    @Override
    fun explain(pw: RelWriter) {
        explainTerms(pw).done(this)
    }

    /**
     * Describes the inputs and attributes of this relational expression.
     * Each node should call `super.explainTerms`, then call the
     * [org.apache.calcite.rel.externalize.RelWriterImpl.input]
     * and
     * [RelWriter.item]
     * methods for each input and attribute.
     *
     * @param pw Plan writer
     * @return Plan writer for fluent-explain pattern
     */
    fun explainTerms(pw: RelWriter): RelWriter {
        return pw
    }

    @Override
    fun onRegister(planner: RelOptPlanner): RelNode {
        val oldInputs: List<RelNode> = inputs
        val inputs: List<RelNode> = ArrayList(oldInputs.size())
        for (input in oldInputs) {
            val e: RelNode = planner.ensureRegistered(input, null)
            assert(
                e === input || RelOptUtil.equal(
                    "rowtype of rel before registration",
                    input.getRowType(),
                    "rowtype of rel after registration",
                    e.getRowType(),
                    Litmus.THROW
                )
            )
            inputs.add(e)
        }
        var r: RelNode = this
        if (!Util.equalShallow(oldInputs, inputs)) {
            r = copy(getTraitSet(), inputs)
        }
        r.recomputeDigest()
        assert(r.isValid(Litmus.THROW, null))
        return r
    }

    @Override
    fun recomputeDigest() {
        digest.clear()
    }

    @Override
    fun replaceInput(
        ordinalInParent: Int,
        p: RelNode?
    ) {
        throw UnsupportedOperationException("replaceInput called on $this")
    }

    /** Description; consists of id plus digest.  */
    @Override
    fun toString(): String {
        return "rel#" + id + ':' + getDigest()
    }

    // to be removed before 2.0
    @get:Override
    @get:Deprecated
    val description: String
    // to be removed before 2.0 get() {
    return this.toString()
}

@Override
fun getDigest(): String {
    return digest.toString()
}

@get:Override
val relDigest: RelDigest
    @Override get() = digest
val table: RelOptTable?
    @Override @Nullable get() = null

/**
 * {@inheritDoc}
 *
 *
 * This method (and [.hashCode] is intentionally final. We do not want
 * sub-classes of [RelNode] to redefine identity. Various algorithms
 * (e.g. visitors, planner) can define the identity as meets their needs.
 */
@Override
fun equals(@Nullable obj: Object?): Boolean {
    return super.equals(obj)
}

/**
 * {@inheritDoc}
 *
 *
 * This method (and [.equals] is intentionally final. We do not want
 * sub-classes of [RelNode] to redefine identity. Various algorithms
 * (e.g. visitors, planner) can define the identity as meets their needs.
 */
@Override
fun hashCode(): Int {
    return super.hashCode()
}

/**
 * Equality check for RelNode digest.
 *
 *
 * By default this method collects digest attributes from
 * [.explainTerms], then compares each attribute pair.
 * This should work well for most cases. If this method is a performance
 * bottleneck for your project, or the default behavior can't handle
 * your scenario properly, you can choose to override this method and
 * [.deepHashCode]. See `LogicalJoin` as an example.
 *
 * @return Whether the 2 RelNodes are equivalent or have the same digest.
 * @see .deepHashCode
 */
@API(since = "1.25", status = API.Status.MAINTAINED)
@Override
fun deepEquals(@Nullable obj: Object?): Boolean {
    if (this === obj) {
        return true
    }
    if (obj == null || this.getClass() !== obj.getClass()) {
        return false
    }
    val that = obj as AbstractRelNode
    var result = (this.getTraitSet().equals(that.getTraitSet())
            && this.getRowType().equalsSansFieldNames(that.getRowType()))
    if (!result) {
        return false
    }
    val items1: List<Pair<String, Object>> = this.digestItems
    val items2: List<Pair<String, Object>> = that.digestItems
    if (items1.size() !== items2.size()) {
        return false
    }
    var i = 0
    while (result && i < items1.size()) {
        val attr1: Pair<String, Object> = items1[i]
        val attr2: Pair<String, Object> = items2[i]
        result = if (attr1.right is RelNode) {
            (attr1.right as RelNode).deepEquals(attr2.right)
        } else {
            attr1.equals(attr2)
        }
        i++
    }
    return result
}

/**
 * Compute hash code for RelNode digest.
 *
 * @see RelNode.deepEquals
 */
@API(since = "1.25", status = API.Status.MAINTAINED)
@Override
fun deepHashCode(): Int {
    var result: Int = 31 + getTraitSet().hashCode()
    val items: List<Pair<String, Object>> = this.digestItems
    for (item in items) {
        val value: Object = item.right
        val h: Int
        h = if (value == null) {
            0
        } else if (value is RelNode) {
            (value as RelNode).deepHashCode()
        } else {
            value.hashCode()
        }
        result = result * 31 + h
    }
    return result
}

private val digestItems: List<Any>
    private get() {
        val rdw: AbstractRelNode.RelDigestWriter = AbstractRelNode.RelDigestWriter()
        explainTerms(rdw)
        if (this is Hintable) {
            val hints: List<RelHint> = (this as Hintable).getHints()
            rdw.itemIf("hints", hints, !hints.isEmpty())
        }
        return rdw.attrs
    }

/** Implementation of [RelDigest].  */
private inner class InnerRelDigest : RelDigest {
    /** Cached hash code.  */
    private var hash = 0
    val rel: org.apache.calcite.rel.RelNode
        @Override get() = this@AbstractRelNode

    @Override
    fun clear() {
        hash = 0
    }

    @Override
    fun equals(@Nullable o: Object?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || getClass() !== o.getClass()) {
            return false
        }
        val relDigest: AbstractRelNode.InnerRelDigest = o as AbstractRelNode.InnerRelDigest
        return deepEquals(relDigest.getRel())
    }

    @Override
    fun hashCode(): Int {
        if (hash == 0) {
            hash = deepHashCode()
        }
        return hash
    }

    @Override
    fun toString(): String {
        val rdw: AbstractRelNode.RelDigestWriter = AbstractRelNode.RelDigestWriter()
        explain(rdw)
        return requireNonNull(rdw.digest, "digest")
    }
}

/**
 * A writer object used exclusively for computing the digest of a RelNode.
 *
 *
 * The writer is meant to be used only for computing a single digest and then thrown away.
 * After calling [.done] the writer should be used only to obtain the computed
 * [.digest]. Any other action is prohibited.
 *
 */
private class RelDigestWriter : RelWriter {
    private val attrs: List<Pair<String, Object>> = ArrayList()

    @Nullable
    var digest: String? = null
    @Override
    fun explain(
        rel: RelNode?,
        valueList: List<Pair<String?, Object?>?>?
    ) {
        throw IllegalStateException("Should not be called for computing digest")
    }

    val detailLevel: SqlExplainLevel
        @Override get() = SqlExplainLevel.DIGEST_ATTRIBUTES

    @Override
    fun item(term: String?, @Nullable value: Object?): RelWriter {
        var value: Object? = value
        if (value != null && value.getClass().isArray()) {
            // We can't call hashCode and equals on Array, so
            // convert it to String to keep the same behaviour.
            value = "" + value
        }
        attrs.add(Pair.of(term, value))
        return this
    }

    @Override
    fun done(node: RelNode): RelWriter {
        val sb = StringBuilder()
        sb.append(node.getRelTypeName())
        sb.append('.')
        sb.append(node.getTraitSet())
        sb.append('(')
        var j = 0
        for (attr in attrs) {
            if (j++ > 0) {
                sb.append(',')
            }
            sb.append(attr.left)
            sb.append('=')
            if (attr.right is RelNode) {
                val input: RelNode = attr.right as RelNode
                sb.append(input.getRelTypeName())
                sb.append('#')
                sb.append(input.getId())
            } else {
                sb.append(attr.right)
            }
        }
        sb.append(')')
        digest = sb.toString()
        return this
    }
}

companion object {
    //~ Static fields/initializers ---------------------------------------------
    /** Generator for [.id] values.  */
    private val NEXT_ID: AtomicInteger = AtomicInteger(0)
    protected fun <T> sole(collection: List<T>): T {
        assert(collection.size() === 1)
        return collection[0]
    }
}
}
