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
package org.apache.calcite.rel.rules

import org.apache.calcite.rel.RelNode

/**
 * Utility class used to store a [org.apache.calcite.rel.core.Join] tree
 * and the factors that make up the tree.
 *
 *
 * Because [RelNode]s can be duplicated in a query
 * when you have a self-join, factor ids are needed to distinguish between the
 * different join inputs that correspond to identical tables. The class
 * associates factor ids with a join tree, matching the order of the factor ids
 * with the order of those factors in the join tree.
 */
class LoptJoinTree {
    //~ Instance fields --------------------------------------------------------
    @NotOnlyInitialized
    val factorTree: BinaryTree
    private val joinTree: RelNode?
    val isRemovableSelfJoin: Boolean
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a join-tree consisting of a single node.
     *
     * @param joinTree RelNode corresponding to the single node
     * @param factorId factor id of the node
     */
    @SuppressWarnings("argument.type.incompatible")
    constructor(joinTree: RelNode?, factorId: Int) {
        this.joinTree = joinTree
        factorTree = Leaf(factorId, this)
        isRemovableSelfJoin = false
    }

    /**
     * Associates the factor ids with a join-tree.
     *
     * @param joinTree RelNodes corresponding to the join tree
     * @param factorTree tree of the factor ids
     * @param removableSelfJoin whether the join corresponds to a removable
     * self-join
     */
    constructor(
        joinTree: RelNode?,
        factorTree: BinaryTree,
        removableSelfJoin: Boolean
    ) {
        this.joinTree = joinTree
        this.factorTree = factorTree
        isRemovableSelfJoin = removableSelfJoin
    }

    /**
     * Associates the factor ids with a join-tree given the factors corresponding
     * to the left and right subtrees of the join.
     *
     * @param joinTree RelNodes corresponding to the join tree
     * @param leftFactorTree tree of the factor ids for left subtree
     * @param rightFactorTree tree of the factor ids for the right subtree
     */
    constructor(
        joinTree: RelNode?,
        leftFactorTree: BinaryTree?,
        rightFactorTree: BinaryTree?
    ) : this(joinTree, leftFactorTree, rightFactorTree, false) {
    }

    /**
     * Associates the factor ids with a join-tree given the factors corresponding
     * to the left and right subtrees of the join. Also indicates whether the
     * join is a removable self-join.
     *
     * @param joinTree RelNodes corresponding to the join tree
     * @param leftFactorTree tree of the factor ids for left subtree
     * @param rightFactorTree tree of the factor ids for the right subtree
     * @param removableSelfJoin true if the join is a removable self-join
     */
    constructor(
        joinTree: RelNode?,
        leftFactorTree: BinaryTree?,
        rightFactorTree: BinaryTree?,
        removableSelfJoin: Boolean
    ) {
        factorTree = Node(leftFactorTree, rightFactorTree, this)
        this.joinTree = joinTree
        isRemovableSelfJoin = removableSelfJoin
    }

    //~ Methods ----------------------------------------------------------------
    fun getJoinTree(): RelNode? {
        return joinTree
    }

    val left: LoptJoinTree
        get() {
            val node = factorTree as Node
            return LoptJoinTree(
                (joinTree as Join?).getLeft(),
                node.left,
                node.left.parent.isRemovableSelfJoin
            )
        }
    val right: LoptJoinTree
        get() {
            val node = factorTree as Node
            return LoptJoinTree(
                (joinTree as Join?).getRight(),
                node.right,
                node.right.parent.isRemovableSelfJoin
            )
        }
    val treeOrder: List<Any>
        get() {
            val treeOrder: List<Integer> = ArrayList()
            getTreeOrder(treeOrder)
            return treeOrder
        }

    fun getTreeOrder(treeOrder: List<Integer?>?) {
        factorTree.getTreeOrder(treeOrder)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Simple binary tree class that stores an id in the leaf nodes and keeps
     * track of the parent LoptJoinTree object associated with the binary tree.
     */
    protected abstract class BinaryTree protected constructor(@field:NotOnlyInitialized @param:UnderInitialization val parent: LoptJoinTree) {

        abstract fun getTreeOrder(treeOrder: List<Integer?>?)
    }

    /** Binary tree node that has no children.  */
    protected class Leaf(
        /** Returns the id associated with a leaf node in a binary tree.  */
        val id: Int, @UnderInitialization parent: LoptJoinTree
    ) : BinaryTree(parent) {

        @Override
        override fun getTreeOrder(treeOrder: List<Integer?>) {
            treeOrder.add(id)
        }
    }

    /** Binary tree node that has two children.  */
    protected class Node(left: BinaryTree?, right: BinaryTree?, @UnderInitialization parent: LoptJoinTree) :
        BinaryTree(parent) {
        val left: BinaryTree
        val right: BinaryTree

        init {
            this.left = Objects.requireNonNull(left, "left")
            this.right = Objects.requireNonNull(right, "right")
        }

        @Override
        override fun getTreeOrder(treeOrder: List<Integer?>?) {
            left.getTreeOrder(treeOrder)
            right.getTreeOrder(treeOrder)
        }
    }
}
