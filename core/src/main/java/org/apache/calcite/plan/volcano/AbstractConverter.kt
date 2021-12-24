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
package org.apache.calcite.plan.volcano

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.RelTraitDef
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.convert.ConverterImpl
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value
import java.util.List

/**
 * Converts a relational expression to any given output convention.
 *
 *
 * Unlike most [org.apache.calcite.rel.convert.Converter]s, an abstract
 * converter is always abstract. You would typically create an
 * `AbstractConverter` when it is necessary to transform a relational
 * expression immediately; later, rules will transform it into relational
 * expressions which can be implemented.
 *
 *
 * If an abstract converter cannot be satisfied immediately (because the
 * source subset is abstract), the set is flagged, so this converter will be
 * expanded as soon as a non-abstract relexp is added to the set.
 */
@Value.Enclosing
class AbstractConverter(
    cluster: RelOptCluster?,
    rel: RelSubset?,
    @Nullable traitDef: RelTraitDef?,
    traits: RelTraitSet
) : ConverterImpl(cluster, traitDef, traits, rel) {
    //~ Constructors -----------------------------------------------------------
    init {
        assert(traits.allSimple())
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>?): RelNode {
        return AbstractConverter(
            getCluster(),
            sole(inputs) as RelSubset?,
            traitDef,
            traitSet
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery?
    ): RelOptCost {
        return planner.getCostFactory().makeInfiniteCost()
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        for (trait in traitSet) {
            pw.item(trait.getTraitDef().getSimpleName(), trait)
        }
        return pw
    }

    @get:Override
    val isEnforcer: Boolean
        get() = true
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Rule that converts an [AbstractConverter] into a chain of
     * converters from the source relation to the target traits.
     *
     *
     * The chain produced is minimal: we have previously built the transitive
     * closure of the graph of conversions, so we choose the shortest chain.
     *
     *
     * Unlike the [AbstractConverter] they are replacing, these
     * converters are guaranteed to be able to convert any relation of their
     * calling convention. Furthermore, because they introduce subsets of other
     * calling conventions along the way, these subsets may spawn more efficient
     * conversions which are not generally applicable.
     *
     *
     * AbstractConverters can be messy, so they restrain themselves: they
     * don't fire if the target subset already has an implementation (with less
     * than infinite cost).
     */
    class ExpandConversionRule
    /** Creates an ExpandConversionRule.  */
    protected constructor(config: Config?) : RelRule<ExpandConversionRule.Config?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(relBuilderFactory: RelBuilderFactory?) : this(
            Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(Config::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val planner: VolcanoPlanner = call.getPlanner() as VolcanoPlanner
            val converter: AbstractConverter = call.rel(0)
            val child: RelNode = converter.getInput()
            val converted: RelNode = planner.changeTraitsUsingConverters(
                child,
                converter.traitSet
            )
            if (converted != null) {
                call.transformTo(converted)
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface Config : RelRule.Config {
            @Override
            fun toRule(): ExpandConversionRule {
                return ExpandConversionRule(this)
            }

            companion object {
                val DEFAULT: Config = ImmutableConverter.Config.of()
                    .withOperandSupplier { b -> b.operand(AbstractConverter::class.java).anyInputs() }
            }
        }

        companion object {
            val INSTANCE = Config.DEFAULT.toRule()
        }
    }
}
