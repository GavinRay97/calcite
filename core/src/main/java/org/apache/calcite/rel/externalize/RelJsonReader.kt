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

import org.apache.calcite.plan.Convention

/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see org.apache.calcite.rel.RelInput
 */
class RelJsonReader(
    cluster: RelOptCluster, relOptSchema: RelOptSchema,
    schema: Schema?
) {
    private val cluster: RelOptCluster
    private val relOptSchema: RelOptSchema
    private val relJson: RelJson = RelJson(null)
    private val relMap: Map<String, RelNode> = LinkedHashMap()

    @Nullable
    private var lastRel: RelNode? = null

    init {
        this.cluster = cluster
        this.relOptSchema = relOptSchema
        Util.discard(schema)
    }

    @Throws(IOException::class)
    fun read(s: String?): RelNode {
        lastRel = null
        val mapper = ObjectMapper()
        val o: Map<String, Object> = mapper
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .readValue(s, TYPE_REF)
        @SuppressWarnings("unchecked") val rels: List<Map<String, Object>> = requireNonNull(o["rels"], "rels") as List
        readRels(rels)
        return requireNonNull(lastRel, "lastRel")
    }

    private fun readRels(jsonRels: List<Map<String, Object>>) {
        for (jsonRel in jsonRels) {
            readRel(jsonRel)
        }
    }

    private fun readRel(jsonRel: Map<String, Object>) {
        val id = requireNonNull(jsonRel["id"], "jsonRel.id") as String
        val type = requireNonNull(jsonRel["relOp"], "jsonRel.relOp") as String
        val constructor: Constructor = relJson.getConstructor(type)
        val input: RelInput = object : RelInput() {
            @get:Override
            val cluster: RelOptCluster

            @get:Override
            val traitSet: RelTraitSet
                get() = cluster.traitSetOf(Convention.NONE)

            @Override
            fun getTable(table: String): RelOptTable {
                val list: List<String> = requireNonNull(
                    getStringList(table)
                ) { "getStringList for $table" }
                return requireNonNull(
                    relOptSchema.getTableForMember(list)
                ) { "table " + table + " is not found in schema " + relOptSchema.toString() }
            }

            @get:Override
            val input: RelNode
                get() {
                    val inputs: List<RelNode> = inputs
                    assert(inputs.size() === 1)
                    return inputs[0]
                }

            @get:Override
            val inputs: List<Any>
                get() {
                    val jsonInputs =
                        getStringList("inputs") ?: return ImmutableList.of(requireNonNull(lastRel, "lastRel"))
                    val inputs: ImmutableList.Builder<RelNode> = Builder()
                    for (jsonInput in jsonInputs) {
                        inputs.add(lookupInput(jsonInput))
                    }
                    return inputs.build()
                }

            @Override
            @Nullable
            fun getExpression(tag: String): RexNode? {
                return relJson.toRex(this, jsonRel[tag])
            }

            @Override
            fun getBitSet(tag: String): ImmutableBitSet {
                return ImmutableBitSet.of(requireNonNull(getIntegerList(tag), tag))
            }

            @Override
            @Nullable
            fun getBitSetList(tag: String): List<ImmutableBitSet>? {
                val list: List<List<Any>> = getIntegerListList(tag) ?: return null
                val builder: ImmutableList.Builder<ImmutableBitSet> = ImmutableList.builder()
                for (integers in list) {
                    builder.add(ImmutableBitSet.of(integers))
                }
                return builder.build()
            }

            @Override
            @Nullable
            fun getStringList(tag: String): List<String>? {
                return jsonRel[tag]
            }

            @Override
            @Nullable
            fun getIntegerList(tag: String): List<Integer>? {
                return jsonRel[tag]
            }

            @Override
            @Nullable
            fun getIntegerListList(tag: String): List<List<Integer>>? {
                return jsonRel[tag]
            }

            @Override
            fun getAggregateCalls(tag: String): List<AggregateCall> {
                val inputs: List<AggregateCall> = ArrayList()
                for (jsonAggCall in getNonNull(tag)) {
                    inputs.add(toAggCall(jsonAggCall))
                }
                return inputs
            }

            @Override
            @Nullable
            operator fun get(tag: String): Object? {
                return jsonRel[tag]
            }

            private fun getNonNull(tag: String): Object {
                return requireNonNull(get(tag)) { "no entry for tag $tag" }
            }

            @Override
            @Nullable
            fun getString(tag: String): String? {
                return get(tag)
            }

            @Override
            fun getFloat(tag: String): Float {
                return (getNonNull(tag) as Number).floatValue()
            }

            @Override
            fun getBoolean(tag: String, default_: Boolean): Boolean {
                val b = get(tag) as Boolean?
                return b ?: default_
            }

            @Override
            fun <E : Enum<E>?> getEnum(tag: String, enumClass: Class<E>?): @Nullable E? {
                return Util.enumVal(
                    enumClass,
                    (getNonNull(tag) as String).toUpperCase(Locale.ROOT)
                )
            }

            @Override
            @Nullable
            fun getExpressionList(tag: String): List<RexNode>? {
                @SuppressWarnings("unchecked") val jsonNodes = jsonRel[tag] as List? ?: return null
                val nodes: List<RexNode> = ArrayList()
                for (jsonNode in jsonNodes) {
                    nodes.add(relJson.toRex(this, jsonNode))
                }
                return nodes
            }

            @Override
            fun getRowType(tag: String): RelDataType {
                val o: Object = getNonNull(tag)
                return relJson.toType(cluster.getTypeFactory(), o)
            }

            @Override
            fun getRowType(expressionsTag: String, fieldsTag: String): RelDataType {
                val expressionList: List<RexNode>? = getExpressionList(expressionsTag)
                @SuppressWarnings("unchecked") val names = getNonNull(fieldsTag) as List<String>
                return cluster.getTypeFactory().createStructType(
                    object : AbstractList<Map.Entry<String?, RelDataType?>?>() {
                        @Override
                        operator fun get(index: Int): Map.Entry<String, RelDataType> {
                            return Pair.of(
                                names[index],
                                requireNonNull(expressionList, "expressionList").get(index).getType()
                            )
                        }

                        @Override
                        fun size(): Int {
                            return names.size()
                        }
                    })
            }

            @get:Override
            val collation: RelCollation
                get() = relJson.toCollation(getNonNull("collation") as List)

            @get:Override
            val distribution: RelDistribution
                get() = relJson.toDistribution(getNonNull("distribution") as Map<String, Object?>)

            @Override
            fun getTuples(tag: String): ImmutableList<ImmutableList<RexLiteral>> {
                val builder: ImmutableList.Builder<ImmutableList<RexLiteral>> = ImmutableList.builder()
                for (jsonTuple in getNonNull(tag)) {
                    builder.add(getTuple(jsonTuple))
                }
                return builder.build()
            }

            fun getTuple(jsonTuple: List): ImmutableList<RexLiteral> {
                val builder: ImmutableList.Builder<RexLiteral> = ImmutableList.builder()
                for (jsonValue in jsonTuple) {
                    builder.add(relJson.toRex(this, jsonValue) as RexLiteral)
                }
                return builder.build()
            }
        }
        lastRel = try {
            val rel: RelNode = constructor.newInstance(input) as RelNode
            relMap.put(id, rel)
            rel
        } catch (e: InstantiationException) {
            throw RuntimeException(e)
        } catch (e: IllegalAccessException) {
            throw RuntimeException(e)
        } catch (e: InvocationTargetException) {
            val e2: Throwable = e.getCause()
            if (e2 is RuntimeException) {
                throw e2 as RuntimeException
            }
            throw RuntimeException(e2)
        }
    }

    private fun toAggCall(jsonAggCall: Map<String, Object>): AggregateCall {
        @SuppressWarnings("unchecked") val aggMap: Map<String, Object> = requireNonNull(
            jsonAggCall["agg"],
            "agg key is not found"
        ) as Map
        val aggregation: SqlAggFunction = requireNonNull(
            relJson.toAggregation(aggMap)
        ) { "relJson.toAggregation output for $aggMap" }
        val distinct = requireNonNull(
            jsonAggCall["distinct"],
            "jsonAggCall.distinct"
        ) as Boolean
        @SuppressWarnings("unchecked") val operands: List<Integer> = requireNonNull(
            jsonAggCall["operands"],
            "jsonAggCall.operands"
        ) as List<Integer>
        val filterOperand: Integer? = jsonAggCall["filter"] as Integer?
        val jsonAggType: Object = requireNonNull(jsonAggCall["type"], "jsonAggCall.type")
        val type: RelDataType = relJson.toType(cluster.getTypeFactory(), jsonAggType)
        val name = jsonAggCall["name"] as String?
        return AggregateCall.create(
            aggregation, distinct, false, false, operands,
            if (filterOperand == null) -1 else filterOperand,
            null, RelCollations.EMPTY, type, name
        )
    }

    private fun lookupInput(jsonInput: String): RelNode {
        return relMap[jsonInput]
            ?: throw RuntimeException(
                "unknown id " + jsonInput
                        + " for relational expression"
            )
    }

    companion object {
        private val TYPE_REF: TypeReference<LinkedHashMap<String, Object>> =
            object : TypeReference<LinkedHashMap<String?, Object?>?>() {}

        /** Converts a JSON string (such as that produced by
         * [RelJson.toJson]) into a Calcite type.  */
        @Throws(IOException::class)
        fun readType(typeFactory: RelDataTypeFactory, s: String?): RelDataType {
            val mapper = ObjectMapper()
            val o: Map<String, Object> = mapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .readValue(s, TYPE_REF)
            return RelJson(null).toType(typeFactory, o)
        }
    }
}
