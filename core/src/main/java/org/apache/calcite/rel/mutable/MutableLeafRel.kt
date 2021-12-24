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

import org.apache.calcite.rel.RelNode

/** Abstract base class for implementations of [MutableRel] that have
 * no inputs.  */
abstract class MutableLeafRel protected constructor(type: MutableRelType?, rel: RelNode?) :
    MutableRel(rel.getCluster(), rel.getRowType(), type) {
    val rel: RelNode?

    init {
        this.rel = rel
    }

    @Override
    override fun setInput(ordinalInParent: Int, input: MutableRel?) {
        throw IllegalArgumentException()
    }

    @get:Override
    override val inputs: List<org.apache.calcite.rel.mutable.MutableRel>
        get() = ImmutableList.of()

    @Override
    override fun childrenAccept(visitor: MutableRelVisitor?) {
        // no children - nothing to do
    }
}
