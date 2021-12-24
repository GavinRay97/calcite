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
package org.apache.calcite.rel.externalize

import org.apache.calcite.avatica.util.Spacer

/**
 * Implementation of [org.apache.calcite.rel.RelWriter].
 */
class RelWriterImpl(
    pw: PrintWriter, detailLevel: SqlExplainLevel,
    withIdPrefix: Boolean
) : RelWriter {
    //~ Instance fields --------------------------------------------------------
    protected val pw: PrintWriter
    protected val detailLevel: SqlExplainLevel
    protected val withIdPrefix: Boolean
    protected val spacer: Spacer = Spacer()
    private val values: List<Pair<String, Object>> = ArrayList()

    //~ Constructors -----------------------------------------------------------
    constructor(pw: PrintWriter) : this(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, true) {}

    init {
        this.pw = pw
        this.detailLevel = detailLevel
        this.withIdPrefix = withIdPrefix
    }

    //~ Methods ----------------------------------------------------------------
    protected fun explain_(
        rel: RelNode,
        values: List<Pair<String?, Object?>?>
    ) {
        val inputs: List<RelNode> = rel.getInputs()
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        if (!mq.isVisibleInExplain(rel, detailLevel)) {
            // render children in place of this, at same level
            explainInputs(inputs)
            return
        }
        val s = StringBuilder()
        spacer.spaces(s)
        if (withIdPrefix) {
            s.append(rel.getId()).append(":")
        }
        s.append(rel.getRelTypeName())
        if (detailLevel !== SqlExplainLevel.NO_ATTRIBUTES) {
            var j = 0
            for (value in values) {
                if (value.right is RelNode) {
                    continue
                }
                if (j++ == 0) {
                    s.append("(")
                } else {
                    s.append(", ")
                }
                s.append(value.left)
                    .append("=[")
                    .append(value.right)
                    .append("]")
            }
            if (j > 0) {
                s.append(")")
            }
        }
        when (detailLevel) {
            ALL_ATTRIBUTES -> s.append(": rowcount = ")
                .append(mq.getRowCount(rel))
                .append(", cumulative cost = ")
                .append(mq.getCumulativeCost(rel))
            else -> {}
        }
        when (detailLevel) {
            NON_COST_ATTRIBUTES, ALL_ATTRIBUTES -> if (!withIdPrefix) {
                // If we didn't print the rel id at the start of the line, print
                // it at the end.
                s.append(", id = ").append(rel.getId())
            }
            else -> {}
        }
        pw.println(s)
        spacer.add(2)
        explainInputs(inputs)
        spacer.subtract(2)
    }

    private fun explainInputs(inputs: List<RelNode>) {
        for (input in inputs) {
            input.explain(this)
        }
    }

    @Override
    fun explain(rel: RelNode, valueList: List<Pair<String?, Object?>?>) {
        explain_(rel, valueList)
    }

    @Override
    fun getDetailLevel(): SqlExplainLevel {
        return detailLevel
    }

    @Override
    fun item(term: String?, @Nullable value: Object?): RelWriter {
        values.add(Pair.of(term, value))
        return this
    }

    @Override
    fun done(node: RelNode): RelWriter {
        assert(checkInputsPresentInExplain(node))
        val valuesCopy: List<Pair<String?, Object?>> = ImmutableList.copyOf(values)
        values.clear()
        explain_(node, valuesCopy)
        pw.flush()
        return this
    }

    private fun checkInputsPresentInExplain(node: RelNode): Boolean {
        var i = 0
        if (values.size() > 0 && values[0].left.equals("subset")) {
            ++i
        }
        for (input in node.getInputs()) {
            assert(values[i].right === input)
            ++i
        }
        return true
    }

    /**
     * Converts the collected terms and values to a string. Does not write to
     * the parent writer.
     */
    fun simple(): String {
        val buf = StringBuilder("(")
        for (ord in Ord.zip(values)) {
            if (ord.i > 0) {
                buf.append(", ")
            }
            buf.append(ord.e.left).append("=[").append(ord.e.right).append("]")
        }
        buf.append(")")
        return buf.toString()
    }
}
