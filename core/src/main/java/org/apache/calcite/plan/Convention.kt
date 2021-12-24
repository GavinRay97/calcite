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
package org.apache.calcite.plan

import org.apache.calcite.rel.RelNode

/**
 * Calling convention trait.
 */
interface Convention : RelTrait {
    val `interface`: Class?
    val name: String?

    /**
     * Given an input and required traits, returns the corresponding
     * enforcer rel nodes, like physical Sort, Exchange etc.
     *
     * @param input The input RelNode
     * @param required The required traits
     * @return Physical enforcer that satisfies the required traitSet,
     * or `null` if trait enforcement is not allowed or the
     * required traitSet can't be satisfied.
     */
    @Nullable
    fun enforce(input: RelNode?, required: RelTraitSet?): RelNode? {
        throw RuntimeException(
            getClass().getName()
                    + "#enforce() is not implemented."
        )
    }

    /**
     * Returns whether we should convert from this convention to
     * `toConvention`. Used by [ConventionTraitDef].
     *
     * @param toConvention Desired convention to convert to
     * @return Whether we should convert from this convention to toConvention
     */
    fun canConvertConvention(toConvention: Convention?): Boolean {
        return false
    }

    /**
     * Returns whether we should convert from this trait set to the other trait
     * set.
     *
     *
     * The convention decides whether it wants to handle other trait
     * conversions, e.g. collation, distribution, etc.  For a given convention, we
     * will only add abstract converters to handle the trait (convention,
     * collation, distribution, etc.) conversions if this function returns true.
     *
     * @param fromTraits Traits of the RelNode that we are converting from
     * @param toTraits Target traits
     * @return Whether we should add converters
     */
    fun useAbstractConvertersForConversion(
        fromTraits: RelTraitSet?,
        toTraits: RelTraitSet?
    ): Boolean {
        return false
    }

    /** Return RelFactories struct for this convention. It can can be used to
     * build RelNode.  */
    val relFactories: Struct?
        get() = RelFactories.DEFAULT_STRUCT

    /**
     * Default implementation.
     */
    class Impl(@get:Override override val name: String, relClass: Class<out RelNode?>) : Convention {
        private val relClass: Class<out RelNode?>

        init {
            this.relClass = relClass
        }

        @Override
        override fun toString(): String {
            return name
        }

        @Override
        fun register(planner: RelOptPlanner?) {
        }

        @Override
        fun satisfies(trait: RelTrait): Boolean {
            return this === trait
        }

        @get:Override
        override val `interface`: Class
            get() = relClass

        @get:Override
        val traitDef: RelTraitDef
            get() = ConventionTraitDef.INSTANCE

        @Override
        @Nullable
        override fun enforce(
            input: RelNode?,
            required: RelTraitSet?
        ): RelNode? {
            return null
        }

        @Override
        override fun canConvertConvention(toConvention: Convention?): Boolean {
            return false
        }

        @Override
        override fun useAbstractConvertersForConversion(
            fromTraits: RelTraitSet?,
            toTraits: RelTraitSet?
        ): Boolean {
            return false
        }
    }

    companion object {
        /**
         * Convention that for a relational expression that does not support any
         * convention. It is not implementable, and has to be transformed to
         * something else in order to be implemented.
         *
         *
         * Relational expressions generally start off in this form.
         *
         *
         * Such expressions always have infinite cost.
         */
        val NONE: Convention = Impl("NONE", RelNode::class.java)
    }
}
