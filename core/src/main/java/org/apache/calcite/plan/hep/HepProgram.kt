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
package org.apache.calcite.plan.hep

import com.google.common.collect.ImmutableList
import java.util.List

/**
 * HepProgram specifies the order in which rules should be attempted by
 * [HepPlanner]. Use [HepProgramBuilder] to create a new
 * instance of HepProgram.
 *
 *
 * Note that the structure of a program is immutable, but the planner uses it
 * as read/write during planning, so a program can only be in use by a single
 * planner at a time.
 */
class HepProgram internal constructor(instructions: List<HepInstruction?>?) {
    //~ Instance fields --------------------------------------------------------
    val instructions: ImmutableList<HepInstruction>
    var matchLimit = 0

    @Nullable
    var matchOrder: HepMatchOrder? = null
    var group: @Nullable HepInstruction.EndGroup? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new empty HepProgram. The program has an initial match order of
     * [org.apache.calcite.plan.hep.HepMatchOrder.DEPTH_FIRST], and an initial
     * match limit of [.MATCH_UNTIL_FIXPOINT].
     */
    init {
        this.instructions = ImmutableList.copyOf(instructions)
    }

    //~ Methods ----------------------------------------------------------------
    fun initialize(clearCache: Boolean) {
        matchLimit = MATCH_UNTIL_FIXPOINT
        matchOrder = HepMatchOrder.DEPTH_FIRST
        group = null
        for (instruction in instructions) {
            instruction.initialize(clearCache)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /**
         * Symbolic constant for matching until no more matches occur.
         */
        val MATCH_UNTIL_FIXPOINT: Int = Integer.MAX_VALUE
        fun builder(): HepProgramBuilder {
            return HepProgramBuilder()
        }
    }
}
