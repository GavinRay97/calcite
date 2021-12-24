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
 * Listener for logging useful debugging information on certain rule events.
 */
class RuleEventLogger : RelOptListener {
    @Override
    fun relEquivalenceFound(event: RelEquivalenceEvent?) {
    }

    @Override
    fun ruleAttempted(event: RuleAttemptedEvent) {
        if (event.isBefore() && LOG.isDebugEnabled()) {
            val call: RelOptRuleCall = event.getRuleCall()
            val ruleArgs: String = Arrays.stream(call.rels)
                .map { rel -> "rel#" + rel.getId().toString() + ":" + rel.getRelTypeName() }
                .collect(Collectors.joining(","))
            LOG.debug("call#{}: Apply rule [{}] to [{}]", call.id, call.getRule(), ruleArgs)
        }
    }

    @Override
    fun ruleProductionSucceeded(event: RuleProductionEvent) {
        if (event.isBefore() && LOG.isDebugEnabled()) {
            val call: RelOptRuleCall = event.getRuleCall()
            val newRel: RelNode = event.getRel()
            val description =
                if (newRel == null) "null" else "rel#" + newRel.getId().toString() + ":" + newRel.getRelTypeName()
            LOG.debug("call#{}: Rule [{}] produced [{}]", call.id, call.getRule(), description)
            if (newRel != null) {
                LOG.debug(
                    FULL, "call#{}: Full plan for [{}]:{}", call.id, description,
                    System.lineSeparator() + RelOptUtil.toString(newRel)
                )
            }
        }
    }

    @Override
    fun relDiscarded(event: RelDiscardedEvent?) {
    }

    @Override
    fun relChosen(event: RelChosenEvent?) {
    }

    companion object {
        private val LOG: Logger = CalciteTrace.getPlannerTracer()
        private val FULL: Marker = MarkerFactory.getMarker("FULL_PLAN")
    }
}
