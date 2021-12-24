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

import kotlin.jvm.JvmOverloads

/**
 * A `RelVisitor` is a Visitor role in the
 * [visitor pattern][org.apache.calcite.util.Glossary.VISITOR_PATTERN] and
 * visits [RelNode] objects as the role of Element. Other components in
 * the pattern: [RelNode.childrenAccept].
 */
abstract class RelVisitor {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    private var root: RelNode? = null
    //~ Methods ----------------------------------------------------------------
    /**
     * Visits a node during a traversal.
     *
     * @param node    Node to visit
     * @param ordinal Ordinal of node within its parent
     * @param parent  Parent of the node, or null if it is the root of the
     * traversal
     */
    fun visit(
        node: RelNode,
        ordinal: Int,
        @Nullable parent: RelNode?
    ) {
        node.childrenAccept(this)
    }

    /**
     * Replaces the root node of this traversal.
     *
     * @param node The new root node
     */
    fun replaceRoot(@Nullable node: RelNode?) {
        root = node
    }

    /**
     * Starts an iteration.
     */
    @Nullable
    fun go(p: RelNode): RelNode? {
        root = p
        visit(p, 0, null)
        return root
    }
}
