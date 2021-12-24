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

import org.apache.calcite.util.mapping.IntPair

/** RelOptReferentialConstraint base implementation.  */
class RelReferentialConstraintImpl private constructor(
    sourceQualifiedName: List<String>,
    targetQualifiedName: List<String>, columnPairs: List<IntPair>
) : RelReferentialConstraint {
    override val sourceQualifiedName: List<String>
        @Override get
    override val targetQualifiedName: List<String>
        @Override get
    private override val columnPairs: List<IntPair>

    init {
        this.sourceQualifiedName = ImmutableList.copyOf(sourceQualifiedName)
        this.targetQualifiedName = ImmutableList.copyOf(targetQualifiedName)
        this.columnPairs = ImmutableList.copyOf(columnPairs)
    }

    @Override
    fun getColumnPairs(): List<IntPair> {
        return columnPairs
    }

    @Override
    override fun toString(): String {
        return ("{ " + sourceQualifiedName + ", " + targetQualifiedName + ", "
                + columnPairs + " }")
    }

    companion object {
        fun of(
            sourceQualifiedName: List<String>,
            targetQualifiedName: List<String>, columnPairs: List<IntPair>
        ): RelReferentialConstraintImpl {
            return RelReferentialConstraintImpl(
                sourceQualifiedName, targetQualifiedName, columnPairs
            )
        }
    }
}
