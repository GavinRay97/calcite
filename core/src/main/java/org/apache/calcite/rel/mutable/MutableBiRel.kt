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
package org.apache.calcite.rel.mutable

import org.apache.calcite.plan.RelOptCluster

/** Mutable equivalent of [org.apache.calcite.rel.BiRel].  */
abstract class MutableBiRel @SuppressWarnings("initialization.invalid.field.write.initialized") protected constructor(
    type: MutableRelType?,
    cluster: RelOptCluster?,
    rowType: RelDataType?,
    left: MutableRel,
    right: MutableRel
) : MutableRel(cluster, rowType, type) {
    protected var left: MutableRel?
    protected var right: MutableRel?

    init {
        this.left = left
        left.parent = this
        left.ordinalInParent = 0
        this.right = right
        right.parent = this
        right.ordinalInParent = 1
    }

    @Override
    override fun setInput(ordinalInParent: Int, input: MutableRel?) {
        if (ordinalInParent > 1) {
            throw IllegalArgumentException()
        }
        if (ordinalInParent == 0) {
            left = input
        } else {
            right = input
        }
        if (input != null) {
            input.parent = this
            input.ordinalInParent = ordinalInParent
        }
    }

    @get:Override
    override val inputs: List<org.apache.calcite.rel.mutable.MutableRel>
        get() = ImmutableList.of(left, right)

    fun getLeft(): MutableRel? {
        return left
    }

    fun getRight(): MutableRel? {
        return right
    }

    @Override
    fun childrenAccept(visitor: MutableRelVisitor) {
        visitor.visit(left)
        visitor.visit(right)
    }
}
