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
import java.util.EventListener
import java.util.EventObject

/**
 * RelOptListener defines an interface for listening to events which occur
 * during the optimization process.
 */
interface RelOptListener : EventListener {
    //~ Methods ----------------------------------------------------------------
    /**
     * Notifies this listener that a relational expression has been registered
     * with a particular equivalence class after an equivalence has been either
     * detected or asserted. Equivalence classes may be either logical (all
     * expressions which yield the same result set) or physical (all expressions
     * which yield the same result set with a particular calling convention).
     *
     * @param event details about the event
     */
    fun relEquivalenceFound(event: RelEquivalenceEvent?)

    /**
     * Notifies this listener that an optimizer rule is being applied to a
     * particular relational expression. This rule is called twice; once before
     * the rule is invoked, and once after. Note that the rel attribute of the
     * event is always the old expression.
     *
     * @param event details about the event
     */
    fun ruleAttempted(event: RuleAttemptedEvent?)

    /**
     * Notifies this listener that an optimizer rule has been successfully
     * applied to a particular relational expression, resulting in a new
     * equivalent expression (relEquivalenceFound will also be called unless the
     * new expression is identical to an existing one). This rule is called
     * twice; once before registration of the new rel, and once after. Note that
     * the rel attribute of the event is always the new expression; to get the
     * old expression, use event.getRuleCall().rels[0].
     *
     * @param event details about the event
     */
    fun ruleProductionSucceeded(event: RuleProductionEvent?)

    /**
     * Notifies this listener that a relational expression is no longer of
     * interest to the planner.
     *
     * @param event details about the event
     */
    fun relDiscarded(event: RelDiscardedEvent?)

    /**
     * Notifies this listener that a relational expression has been chosen as
     * part of the final implementation of the query plan. After the plan is
     * complete, this is called one more time with null for the rel.
     *
     * @param event details about the event
     */
    fun relChosen(event: RelChosenEvent?)
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Event class for abstract event dealing with a relational expression. The
     * source of an event is typically the RelOptPlanner which initiated it.
     */
    abstract class RelEvent protected constructor(eventSource: Object?, @Nullable rel: RelNode) :
        EventObject(eventSource) {
        @Nullable
        private val rel: RelNode

        init {
            this.rel = rel
        }

        @Nullable
        fun getRel(): RelNode {
            return rel
        }
    }

    /** Event indicating that a relational expression has been chosen.  */
    class RelChosenEvent(eventSource: Object?, @Nullable rel: RelNode) : RelEvent(eventSource, rel)

    /** Event indicating that a relational expression has been found to
     * be equivalent to an equivalence class.  */
    class RelEquivalenceEvent(
        eventSource: Object?,
        rel: RelNode,
        equivalenceClass: Object,
        isPhysical: Boolean
    ) : RelEvent(eventSource, rel) {
        private val equivalenceClass: Object
        val isPhysical: Boolean

        init {
            this.equivalenceClass = equivalenceClass
            this.isPhysical = isPhysical
        }

        fun getEquivalenceClass(): Object {
            return equivalenceClass
        }
    }

    /** Event indicating that a relational expression has been discarded.  */
    class RelDiscardedEvent(eventSource: Object?, rel: RelNode) : RelEvent(eventSource, rel)

    /** Event indicating that a planner rule has fired.  */
    abstract class RuleEvent protected constructor(
        eventSource: Object?,
        rel: RelNode,
        ruleCall: RelOptRuleCall
    ) : RelEvent(eventSource, rel) {
        private val ruleCall: RelOptRuleCall

        init {
            this.ruleCall = ruleCall
        }

        fun getRuleCall(): RelOptRuleCall {
            return ruleCall
        }
    }

    /** Event indicating that a planner rule has been attempted.  */
    class RuleAttemptedEvent(
        eventSource: Object?,
        rel: RelNode,
        ruleCall: RelOptRuleCall,
        val isBefore: Boolean
    ) : RuleEvent(eventSource, rel, ruleCall)

    /** Event indicating that a planner rule has produced a result.  */
    class RuleProductionEvent(
        eventSource: Object?,
        rel: RelNode,
        ruleCall: RelOptRuleCall,
        before: Boolean
    ) : RuleAttemptedEvent(eventSource, rel, ruleCall, before)
}
