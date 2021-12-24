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

import org.apache.calcite.rel.RelNode

/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
class RelJsonWriter @JvmOverloads constructor(jsonBuilder: JsonBuilder = JsonBuilder()) : RelWriter {
    //~ Instance fields ----------------------------------------------------------
    protected val jsonBuilder: JsonBuilder
    protected val relJson: RelJson
    private val relIdMap: IdentityHashMap<RelNode, String> = IdentityHashMap()
    protected val relList: List<Object>
    private val values: List<Pair<String, Object>> = ArrayList()

    @Nullable
    private var previousId: String? = null

    //~ Constructors -------------------------------------------------------------
    init {
        this.jsonBuilder = jsonBuilder
        relList = this.jsonBuilder.list()
        relJson = RelJson(this.jsonBuilder)
    }

    //~ Methods ------------------------------------------------------------------
    protected fun explain_(rel: RelNode, values: List<Pair<String?, Object?>?>) {
        val map: Map<String, Object> = jsonBuilder.map()
        map.put("id", null) // ensure that id is the first attribute
        map.put("relOp", relJson.classToTypeName(rel.getClass()))
        for (value in values) {
            if (value.right is RelNode) {
                continue
            }
            put(map, value.left, value.right)
        }
        // omit 'inputs: ["3"]' if "3" is the preceding rel
        val list: List<Object> = explainInputs(rel.getInputs())
        if (list.size() !== 1 || !Objects.equals(list[0], previousId)) {
            map.put("inputs", list)
        }
        val id: String = Integer.toString(relIdMap.size())
        relIdMap.put(rel, id)
        map.put("id", id)
        relList.add(map)
        previousId = id
    }

    private fun put(map: Map<String, Object>, name: String, @Nullable value: Object) {
        map.put(name, relJson.toJson(value))
    }

    private fun explainInputs(inputs: List<RelNode>): List<Object> {
        val list: List<Object> = jsonBuilder.list()
        for (input in inputs) {
            var id: String? = relIdMap.get(input)
            if (id == null) {
                input.explain(this)
                id = previousId
            }
            list.add(id)
        }
        return list
    }

    @Override
    fun explain(rel: RelNode, valueList: List<Pair<String?, Object?>?>) {
        explain_(rel, valueList)
    }

    @get:Override
    val detailLevel: SqlExplainLevel
        get() = SqlExplainLevel.ALL_ATTRIBUTES

    @Override
    fun item(term: String?, @Nullable value: Object?): RelWriter {
        values.add(Pair.of(term, value))
        return this
    }

    @Override
    fun done(node: RelNode): RelWriter {
        val valuesCopy: List<Pair<String?, Object?>> = ImmutableList.copyOf(values)
        values.clear()
        explain_(node, valuesCopy)
        return this
    }

    @Override
    fun nest(): Boolean {
        return true
    }

    /**
     * Returns a JSON string describing the relational expressions that were just
     * explained.
     */
    fun asString(): String {
        val map: Map<String, Object> = jsonBuilder.map()
        map.put("rels", relList)
        return jsonBuilder.toJsonString(map)
    }
}
