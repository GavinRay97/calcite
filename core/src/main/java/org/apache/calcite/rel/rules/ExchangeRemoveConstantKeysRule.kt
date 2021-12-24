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

import org.apache.calcite.plan.RelOptPredicateList

/**
 * Planner rule that removes keys from
 * a [Exchange] if those keys are known to be constant.
 *
 *
 * For example,
 * `SELECT key,value FROM (SELECT 1 AS key, value FROM src) r DISTRIBUTE
 * BY key` can be reduced to
 * `SELECT 1 AS key, value FROM src`.
 *
 * @see CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS
 *
 * @see CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS
 */
@Value.Enclosing
class ExchangeRemoveConstantKeysRule
/** Creates an ExchangeRemoveConstantKeysRule.  */
protected constructor(config: Config?) : RelRule<ExchangeRemoveConstantKeysRule.Config?>(config), SubstitutionRule {
    @Override
    fun onMatch(call: RelOptRuleCall?) {
        config.matchHandler().accept(this, call)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ExchangeRemoveConstantKeysRule {
            return ExchangeRemoveConstantKeysRule(this)
        }

        /** Forwards a call to [.onMatch].  */
        @Value.Parameter
        fun matchHandler(): MatchHandler<ExchangeRemoveConstantKeysRule?>?

        /** Sets [.matchHandler].  */
        fun withMatchHandler(matchHandler: MatchHandler<ExchangeRemoveConstantKeysRule?>?): Config?

        /** Defines an operand tree for the given classes.  */
        fun <R : Exchange?> withOperandFor(
            exchangeClass: Class<R>?,
            predicate: Predicate<R>?
        ): Config? {
            return withOperandSupplier { b ->
                b.operand(exchangeClass).predicate(predicate)
                    .anyInputs()
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableExchangeRemoveConstantKeysRule.Config
                .of { rule: ExchangeRemoveConstantKeysRule, call: RelOptRuleCall -> matchExchange(rule, call) }
                .withOperandFor(
                    LogicalExchange::class.java
                ) { exchange ->
                    (exchange.getDistribution().getType()
                            === RelDistribution.Type.HASH_DISTRIBUTED)
                }
            val SORT: Config = ImmutableExchangeRemoveConstantKeysRule.Config
                .of { rule: ExchangeRemoveConstantKeysRule, call: RelOptRuleCall -> matchSortExchange(rule, call) }
                .withDescription("SortExchangeRemoveConstantKeysRule")
                .`as`(Config::class.java)
                .withOperandFor(
                    LogicalSortExchange::class.java
                ) { sortExchange ->
                    (sortExchange.getDistribution().getType()
                            === RelDistribution.Type.HASH_DISTRIBUTED
                            || !sortExchange.getCollation().getFieldCollations()
                        .isEmpty())
                }
        }
    }

    companion object {
        /** Removes constant in distribution keys.  */
        protected fun simplifyDistributionKeys(
            distribution: RelDistribution,
            constants: Set<Integer?>
        ): List<Integer> {
            return distribution.getKeys().stream()
                .filter { key -> !constants.contains(key) }
                .collect(Collectors.toList())
        }

        private fun matchExchange(
            rule: ExchangeRemoveConstantKeysRule,
            call: RelOptRuleCall
        ) {
            val exchange: Exchange = call.rel(0)
            val mq: RelMetadataQuery = call.getMetadataQuery()
            val input: RelNode = exchange.getInput()
            val predicates: RelOptPredicateList = mq.getPulledUpPredicates(input)
            if (RelOptPredicateList.isEmpty(predicates)) {
                return
            }
            val constants: Set<Integer> = HashSet()
            predicates.constantMap.keySet().forEach { key ->
                if (key is RexInputRef) {
                    constants.add((key as RexInputRef).getIndex())
                }
            }
            if (constants.isEmpty()) {
                return
            }
            val distributionKeys: List<Integer> = simplifyDistributionKeys(
                exchange.getDistribution(), constants
            )
            if (distributionKeys.size() !== exchange.getDistribution().getKeys()
                    .size()
            ) {
                call.transformTo(
                    call.builder()
                        .push(exchange.getInput())
                        .exchange(
                            if (distributionKeys.isEmpty()) RelDistributions.SINGLETON else RelDistributions.hash(
                                distributionKeys
                            )
                        )
                        .build()
                )
                call.getPlanner().prune(exchange)
            }
        }

        private fun matchSortExchange(
            rule: ExchangeRemoveConstantKeysRule,
            call: RelOptRuleCall
        ) {
            val sortExchange: SortExchange = call.rel(0)
            val mq: RelMetadataQuery = call.getMetadataQuery()
            val input: RelNode = sortExchange.getInput()
            val predicates: RelOptPredicateList = mq.getPulledUpPredicates(input)
            if (RelOptPredicateList.isEmpty(predicates)) {
                return
            }
            val constants: Set<Integer> = HashSet()
            predicates.constantMap.keySet().forEach { key ->
                if (key is RexInputRef) {
                    constants.add((key as RexInputRef).getIndex())
                }
            }
            if (constants.isEmpty()) {
                return
            }
            var distributionKeys: List<Integer?> = ArrayList()
            var distributionSimplified = false
            val hashDistribution = (sortExchange.getDistribution().getType()
                    === RelDistribution.Type.HASH_DISTRIBUTED)
            if (hashDistribution) {
                distributionKeys = simplifyDistributionKeys(
                    sortExchange.getDistribution(), constants
                )
                distributionSimplified = distributionKeys.size() !== sortExchange.getDistribution().getKeys()
                    .size()
            }
            val fieldCollations: List<RelFieldCollation> = sortExchange
                .getCollation().getFieldCollations().stream().filter { fc -> !constants.contains(fc.getFieldIndex()) }
                .collect(Collectors.toList())
            val collationSimplified = fieldCollations.size() !== sortExchange.getCollation()
                .getFieldCollations().size()
            if (distributionSimplified
                || collationSimplified
            ) {
                val distribution: RelDistribution =
                    if (distributionSimplified) if (distributionKeys.isEmpty()) RelDistributions.SINGLETON else RelDistributions.hash(
                        distributionKeys
                    ) else sortExchange.getDistribution()
                val collation: RelCollation =
                    if (collationSimplified) RelCollations.of(fieldCollations) else sortExchange.getCollation()
                call.transformTo(
                    call.builder()
                        .push(sortExchange.getInput())
                        .sortExchange(distribution, collation)
                        .build()
                )
                call.getPlanner().prune(sortExchange)
            }
        }
    }
}
