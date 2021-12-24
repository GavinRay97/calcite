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
package org.apache.calcite.rel.metadata

import org.apache.calcite.plan.volcano.RelSubset

/**
 * RelMdNodeTypeCount supplies a default implementation of
 * [RelMetadataQuery.getNodeTypes] for the standard logical algebra.
 */
class RelMdNodeTypes : MetadataHandler<BuiltInMetadata.NodeTypes?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.NodeTypes.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.NodeTypes.getNodeTypes],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getNodeTypes
     */
    @Nullable
    fun getNodeTypes(
        rel: RelNode,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, RelNode::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: RelSubset,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode>? {
        val bestOrOriginal: RelNode = Util.first(rel.getBest(), rel.getOriginal()) ?: return null
        return mq.getNodeTypes(bestOrOriginal)
    }

    @Nullable
    fun getNodeTypes(
        rel: Union,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Union::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Intersect,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Intersect::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Minus,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Minus::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Filter,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Filter::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Calc,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Calc::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Project,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Project::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Sort,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Sort::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Join,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Join::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Aggregate,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Aggregate::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: TableScan,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, TableScan::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Values,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Values::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: TableModify,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, TableModify::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Exchange,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Exchange::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Sample,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Sample::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Correlate,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Correlate::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Window,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Window::class.java, mq)
    }

    @Nullable
    fun getNodeTypes(
        rel: Match,
        mq: RelMetadataQuery
    ): Multimap<Class<out RelNode?>, RelNode> {
        return getNodeTypes(rel, Match::class.java, mq)
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdNodeTypes(), BuiltInMetadata.NodeTypes.Handler::class.java
        )

        @Nullable
        private fun getNodeTypes(
            rel: RelNode,
            c: Class<out RelNode?>, mq: RelMetadataQuery
        ): Multimap<Class<out RelNode?>, RelNode>? {
            val nodeTypeCount: Multimap<Class<out RelNode?>, RelNode> = ArrayListMultimap.create()
            for (input in rel.getInputs()) {
                val partialNodeTypeCount: Multimap<Class<out RelNode?>, RelNode> = mq.getNodeTypes(input) ?: return null
                nodeTypeCount.putAll(partialNodeTypeCount)
            }
            nodeTypeCount.put(c, rel)
            return nodeTypeCount
        }
    }
}
