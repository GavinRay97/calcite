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
package org.apache.calcite.tools

import org.apache.calcite.adapter.enumerable.EnumerableRules

/**
 * Utilities for creating [Program]s.
 */
object Programs {
    @Deprecated // to be removed before 2.0
    val CALC_RULES: ImmutableList<RelOptRule> = RelOptRules.CALC_RULES

    /** Program that converts filters and projects to [Calc]s.  */
    val CALC_PROGRAM: Program = calc(DefaultRelMetadataProvider.INSTANCE)

    /** Program that expands sub-queries.  */
    val SUB_QUERY_PROGRAM: Program = subQuery(DefaultRelMetadataProvider.INSTANCE)
    val RULE_SET: ImmutableSet<RelOptRule> = ImmutableSet.of(
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_CORRELATE_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_FILTER_RULE,
        EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_LIMIT_RULE,
        EnumerableRules.ENUMERABLE_UNION_RULE,
        EnumerableRules.ENUMERABLE_MERGE_UNION_RULE,
        EnumerableRules.ENUMERABLE_INTERSECT_RULE,
        EnumerableRules.ENUMERABLE_MINUS_RULE,
        EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
        EnumerableRules.ENUMERABLE_VALUES_RULE,
        EnumerableRules.ENUMERABLE_WINDOW_RULE,
        EnumerableRules.ENUMERABLE_MATCH_RULE,
        CoreRules.PROJECT_TO_SEMI_JOIN,
        CoreRules.JOIN_TO_SEMI_JOIN,
        CoreRules.MATCH,
        if (CalciteSystemProperty.COMMUTE.value()) CoreRules.JOIN_ASSOCIATE else CoreRules.PROJECT_MERGE,
        CoreRules.AGGREGATE_STAR_TABLE,
        CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
        CoreRules.FILTER_SCAN,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.JOIN_COMMUTE,
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT,
        CoreRules.SORT_PROJECT_TRANSPOSE
    )

    /** Creates a program that executes a rule set.  */
    fun of(ruleSet: RuleSet): Program {
        return RuleSetProgram(ruleSet)
    }

    /** Creates a list of programs based on an array of rule sets.  */
    fun listOf(vararg ruleSets: RuleSet?): List<Program> {
        return Util.transform(Arrays.asList(ruleSets), Programs::of)
    }

    /** Creates a list of programs based on a list of rule sets.  */
    fun listOf(ruleSets: List<RuleSet?>?): List<Program> {
        return Util.transform(ruleSets, Programs::of)
    }

    /** Creates a program from a list of rules.  */
    fun ofRules(vararg rules: RelOptRule?): Program {
        return of(RuleSets.ofList(rules))
    }

    /** Creates a program from a list of rules.  */
    fun ofRules(rules: Iterable<RelOptRule?>?): Program {
        return of(RuleSets.ofList(rules))
    }

    /** Creates a program that executes a sequence of programs.  */
    fun sequence(vararg programs: Program?): Program {
        return SequenceProgram(ImmutableList.copyOf(programs))
    }

    /** Creates a program that executes a list of rules in a HEP planner.  */
    fun hep(
        rules: Iterable<RelOptRule?>,
        noDag: Boolean, metadataProvider: RelMetadataProvider?
    ): Program {
        val builder: HepProgramBuilder = HepProgram.builder()
        for (rule in rules) {
            builder.addRuleInstance(rule)
        }
        return of(builder.build(), noDag, metadataProvider)
    }

    /** Creates a program that executes a [HepProgram].  */
    @SuppressWarnings("deprecation")
    fun of(
        hepProgram: HepProgram?, noDag: Boolean,
        metadataProvider: RelMetadataProvider?
    ): Program {
        return Program { planner, rel, requiredOutputTraits, materializations, lattices ->
            val hepPlanner = HepPlanner(
                hepProgram,
                null, noDag, null, RelOptCostImpl.FACTORY
            )
            val list: List<RelMetadataProvider> = ArrayList()
            if (metadataProvider != null) {
                list.add(metadataProvider)
            }
            hepPlanner.registerMetadataProviders(list)
            for (materialization in materializations) {
                hepPlanner.addMaterialization(materialization)
            }
            for (lattice in lattices) {
                hepPlanner.addLattice(lattice)
            }
            val plannerChain: RelMetadataProvider = ChainedRelMetadataProvider.of(list)
            rel.getCluster().setMetadataProvider(plannerChain)
            hepPlanner.setRoot(rel)
            hepPlanner.findBestExp()
        }
    }

    /** Creates a program that invokes heuristic join-order optimization
     * (via [org.apache.calcite.rel.rules.JoinToMultiJoinRule],
     * [org.apache.calcite.rel.rules.MultiJoin] and
     * [org.apache.calcite.rel.rules.LoptOptimizeJoinRule])
     * if there are 6 or more joins (7 or more relations).  */
    fun heuristicJoinOrder(
        rules: Iterable<RelOptRule?>?,
        bushy: Boolean, minJoinCount: Int
    ): Program {
        return Program { planner, rel, requiredOutputTraits, materializations, lattices ->
            val joinCount: Int = RelOptUtil.countJoins(rel)
            val program: Program
            program = if (joinCount < minJoinCount) {
                ofRules(rules)
            } else {
                // Create a program that gathers together joins as a MultiJoin.
                val hep: HepProgram = HepProgramBuilder()
                    .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                    .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                    .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
                    .build()
                val program1: Program = of(hep, false, DefaultRelMetadataProvider.INSTANCE)

                // Create a program that contains a rule to expand a MultiJoin
                // into heuristically ordered joins.
                // We use the rule set passed in, but remove JoinCommuteRule and
                // JoinPushThroughJoinRule, because they cause exhaustive search.
                val list: List<RelOptRule> = Lists.newArrayList(rules)
                list.removeAll(
                    ImmutableList.of(
                        CoreRules.JOIN_COMMUTE,
                        CoreRules.JOIN_ASSOCIATE,
                        JoinPushThroughJoinRule.LEFT,
                        JoinPushThroughJoinRule.RIGHT
                    )
                )
                list.add(if (bushy) CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY else CoreRules.MULTI_JOIN_OPTIMIZE)
                val program2: Program = ofRules(list)
                sequence(program1, program2)
            }
            program.run(
                planner, rel, requiredOutputTraits, materializations, lattices
            )
        }
    }

    fun calc(metadataProvider: RelMetadataProvider?): Program {
        return hep(RelOptRules.CALC_RULES, true, metadataProvider)
    }

    @Deprecated // to be removed before 2.0
    fun subquery(metadataProvider: RelMetadataProvider?): Program {
        return subQuery(metadataProvider)
    }

    fun subQuery(metadataProvider: RelMetadataProvider?): Program {
        val builder: HepProgramBuilder = HepProgram.builder()
        builder.addRuleCollection(
            ImmutableList.of(
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
            )
        )
        return of(builder.build(), true, metadataProvider)
    }

    @get:Deprecated
    val program: org.apache.calcite.tools.Program
        get() = Program { planner, rel, requiredOutputTraits, materializations, lattices -> castNonNull(null) }

    /** Returns the standard program used by Prepare.  */
    fun standard(): Program {
        return standard(DefaultRelMetadataProvider.INSTANCE)
    }

    /** Returns the standard program with user metadata provider.  */
    fun standard(metadataProvider: RelMetadataProvider?): Program {
        val program1 = Program { planner, rel, requiredOutputTraits, materializations, lattices ->
            for (materialization in materializations) {
                planner.addMaterialization(materialization)
            }
            for (lattice in lattices) {
                planner.addLattice(lattice)
            }
            planner.setRoot(rel)
            val rootRel2: RelNode = if (rel.getTraitSet().equals(requiredOutputTraits)) rel else planner.changeTraits(
                rel,
                requiredOutputTraits
            )
            assert(rootRel2 != null)
            planner.setRoot(rootRel2)
            val planner2: RelOptPlanner = planner.chooseDelegate()
            val rootRel3: RelNode = planner2.findBestExp()
            assert(rootRel3 != null) { "could not implement exp" }
            rootRel3
        }
        return sequence(
            subQuery(metadataProvider),
            DecorrelateProgram(),
            TrimFieldsProgram(),
            program1,  // Second planner pass to do physical "tweaks". This the first time
            // that EnumerableCalcRel is introduced.
            calc(metadataProvider)
        )
    }

    /** Program backed by a [RuleSet].  */
    internal class RuleSetProgram(ruleSet: RuleSet) : Program {
        val ruleSet: RuleSet

        init {
            this.ruleSet = ruleSet
        }

        @Override
        fun run(
            planner: RelOptPlanner, rel: RelNode,
            requiredOutputTraits: RelTraitSet?,
            materializations: List<RelOptMaterialization?>,
            lattices: List<RelOptLattice?>
        ): RelNode {
            var rel: RelNode = rel
            planner.clear()
            for (rule in ruleSet) {
                planner.addRule(rule)
            }
            for (materialization in materializations) {
                planner.addMaterialization(materialization)
            }
            for (lattice in lattices) {
                planner.addLattice(lattice)
            }
            if (!rel.getTraitSet().equals(requiredOutputTraits)) {
                rel = planner.changeTraits(rel, requiredOutputTraits)
            }
            planner.setRoot(rel)
            return planner.findBestExp()
        }
    }

    /** Program that runs sub-programs, sending the output of the previous as
     * input to the next.  */
    private class SequenceProgram internal constructor(programs: ImmutableList<Program?>) : Program {
        private val programs: ImmutableList<Program>

        init {
            this.programs = programs
        }

        @Override
        override fun run(
            planner: RelOptPlanner?, rel: RelNode?,
            requiredOutputTraits: RelTraitSet?,
            materializations: List<RelOptMaterialization?>?,
            lattices: List<RelOptLattice?>?
        ): RelNode? {
            var rel: RelNode? = rel
            for (program in programs) {
                rel = program.run(
                    planner, rel, requiredOutputTraits, materializations, lattices
                )
            }
            return rel
        }
    }

    /** Program that de-correlates a query.
     *
     *
     * To work around
     * [[CALCITE-842]
 * Decorrelator gets field offsets confused if fields have been trimmed](https://issues.apache.org/jira/browse/CALCITE-842),
     * disable field-trimming in [SqlToRelConverter], and run
     * [TrimFieldsProgram] after this program.  */
    private class DecorrelateProgram : Program {
        @Override
        override fun run(
            planner: RelOptPlanner, rel: RelNode,
            requiredOutputTraits: RelTraitSet?,
            materializations: List<RelOptMaterialization?>?,
            lattices: List<RelOptLattice?>?
        ): RelNode {
            val config: CalciteConnectionConfig = planner.getContext().maybeUnwrap(CalciteConnectionConfig::class.java)
                .orElse(CalciteConnectionConfig.DEFAULT)
            if (config.forceDecorrelate()) {
                val relBuilder: RelBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null)
                return RelDecorrelator.decorrelateQuery(rel, relBuilder)
            }
            return rel
        }
    }

    /** Program that trims fields.  */
    private class TrimFieldsProgram : Program {
        @Override
        override fun run(
            planner: RelOptPlanner?, rel: RelNode,
            requiredOutputTraits: RelTraitSet?,
            materializations: List<RelOptMaterialization?>?,
            lattices: List<RelOptLattice?>?
        ): RelNode {
            val relBuilder: RelBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null)
            return RelFieldTrimmer(null, relBuilder).trim(rel)
        }
    }
}
