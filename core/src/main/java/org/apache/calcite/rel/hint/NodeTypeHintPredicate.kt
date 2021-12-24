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
package org.apache.calcite.rel.hint

import org.apache.calcite.rel.RelNode

/**
 * A hint predicate that specifies which kind of relational
 * expression the hint can be applied to.
 */
class NodeTypeHintPredicate(private val nodeType: NodeType) : HintPredicate {
    /**
     * Enumeration of the relational expression types that the hints
     * may be propagated to.
     */
    enum class NodeType(relClazz: Class<*>) {
        /**
         * The hint is used for the whole query, kind of like a query config.
         * This kind of hints would never be propagated.
         */
        SET_VAR(RelNode::class.java),

        /**
         * The hint would be propagated to the Join nodes.
         */
        JOIN(Join::class.java),

        /**
         * The hint would be propagated to the TableScan nodes.
         */
        TABLE_SCAN(TableScan::class.java),

        /**
         * The hint would be propagated to the Project nodes.
         */
        PROJECT(Project::class.java),

        /**
         * The hint would be propagated to the Aggregate nodes.
         */
        AGGREGATE(Aggregate::class.java),

        /**
         * The hint would be propagated to the Calc nodes.
         */
        CALC(Calc::class.java);

        /** Relational expression clazz that the hint can apply to.  */
        @SuppressWarnings("ImmutableEnumChecker")
        val relClazz: Class<*>

        init {
            this.relClazz = relClazz
        }
    }

    @Override
    override fun apply(hint: RelHint?, rel: RelNode?): Boolean {
        return when (nodeType) {
            NodeType.SET_VAR -> false
            else -> nodeType.relClazz.isInstance(rel)
        }
    }
}
