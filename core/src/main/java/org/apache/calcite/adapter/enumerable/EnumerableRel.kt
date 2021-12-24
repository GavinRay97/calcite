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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.linq4j.tree.BlockStatement

/**
 * A relational expression of one of the
 * [org.apache.calcite.adapter.enumerable.EnumerableConvention] calling
 * conventions.
 */
interface EnumerableRel : PhysicalNode {
    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet?, List<RelTraitSet?>?>? {
        return null
    }

    @Override
    @Nullable
    fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet?, List<RelTraitSet?>?>? {
        return null
    }

    @get:Override
    val deriveMode: DeriveMode?
        get() = DeriveMode.LEFT_FIRST

    /**
     * Creates a plan for this expression according to a calling convention.
     *
     * @param implementor Implementor
     * @param pref Preferred representation for rows in result expression
     * @return Plan for this expression according to a calling convention
     */
    fun implement(implementor: EnumerableRelImplementor?, pref: Prefer?): Result

    /** Preferred physical type.  */
    enum class Prefer {
        /** Records must be represented as arrays.  */
        ARRAY,

        /** Consumer would prefer that records are represented as arrays, but can
         * accommodate records represented as objects.  */
        ARRAY_NICE,

        /** Records must be represented as objects.  */
        CUSTOM,

        /** Consumer would prefer that records are represented as objects, but can
         * accommodate records represented as arrays.  */
        CUSTOM_NICE,

        /** Consumer has no preferred representation.  */
        ANY;

        fun preferCustom(): JavaRowFormat {
            return prefer(JavaRowFormat.CUSTOM)
        }

        fun preferArray(): JavaRowFormat {
            return prefer(JavaRowFormat.ARRAY)
        }

        fun prefer(format: JavaRowFormat): JavaRowFormat {
            return when (this) {
                CUSTOM -> JavaRowFormat.CUSTOM
                ARRAY -> JavaRowFormat.ARRAY
                else -> format
            }
        }

        fun of(format: JavaRowFormat?): Prefer {
            return when (format) {
                ARRAY -> ARRAY
                else -> CUSTOM
            }
        }
    }

    /** Result of implementing an enumerable relational expression by generating
     * Java code.  */
    class Result(
        block: BlockStatement, physType: PhysType,
        format: JavaRowFormat
    ) {
        val block: BlockStatement

        /**
         * Describes the Java type returned by this relational expression, and the
         * mapping between it and the fields of the logical row type.
         */
        val physType: PhysType
        val format: JavaRowFormat

        init {
            this.block = block
            this.physType = physType
            this.format = format
        }
    }
}
