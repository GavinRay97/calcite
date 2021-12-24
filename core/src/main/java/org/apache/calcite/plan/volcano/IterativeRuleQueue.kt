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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules.SubstitutionRule
import org.apache.calcite.util.trace.CalciteTrace
import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import org.slf4j.Logger
import java.io.PrintWriter
import java.io.StringWriter
import java.util.ArrayDeque
import java.util.HashSet
import java.util.Queue
import java.util.Set

/**
 * Priority queue of relexps whose rules have not been called, and rule-matches
 * which have not yet been acted upon.
 */
internal class IterativeRuleQueue  //~ Constructors -----------------------------------------------------------
    (planner: VolcanoPlanner?) : RuleQueue(planner) {
    //~ Instance fields --------------------------------------------------------
    /**
     * The list of rule-matches. Initially, there is an empty [MatchList].
     * As the planner invokes [.addMatch] the rule-match
     * is added to the appropriate MatchList(s). As the planner completes the
     * match, the matching entry is removed from this list to avoid unused work.
     */
    val matchList = MatchList()
    //~ Methods ----------------------------------------------------------------
    /**
     * Clear internal data structure for this rule queue.
     */
    @Override
    fun clear(): Boolean {
        var empty = true
        if (!matchList.queue.isEmpty() || !matchList.preQueue.isEmpty()) {
            empty = false
        }
        matchList.clear()
        return !empty
    }

    /**
     * Add a rule match.
     */
    @Override
    fun addMatch(match: VolcanoRuleMatch) {
        val matchName: String = match.toString()
        if (!matchList.names.add(matchName)) {
            // Identical match has already been added.
            return
        }
        LOGGER.trace("Rule-match queued: {}", matchName)
        matchList.offer(match)
        matchList.matchMap.put(
            planner.getSubset(match.rels.get(0)), match
        )
    }

    /**
     * Removes the rule match from the head of match list, and returns it.
     *
     *
     * Returns `null` if there are no more matches.
     *
     *
     * Note that the VolcanoPlanner may still decide to reject rule matches
     * which have become invalid, say if one of their operands belongs to an
     * obsolete set or has been pruned.
     *
     */
    @Nullable
    fun popMatch(): VolcanoRuleMatch? {
        dumpPlannerState()
        var match: VolcanoRuleMatch?
        while (true) {
            if (matchList.size() == 0) {
                return null
            }
            dumpRuleQueue(matchList)
            match = matchList.poll()
            if (match == null) {
                return null
            }
            if (skipMatch(match)) {
                LOGGER.debug("Skip match: {}", match)
            } else {
                break
            }
        }

        // If sets have merged since the rule match was enqueued, the match
        // may not be removed from the matchMap because the subset may have
        // changed, it is OK to leave it since the matchMap will be cleared
        // at the end.
        matchList.matchMap.remove(
            planner.getSubset(match.rels.get(0)), match
        )
        LOGGER.debug("Pop match: {}", match)
        return match
    }

    /**
     * Dumps planner's state to the logger when debug level is set to `TRACE`.
     */
    private fun dumpPlannerState() {
        if (LOGGER.isTraceEnabled()) {
            val sw = StringWriter()
            val pw = PrintWriter(sw)
            planner.dump(pw)
            pw.flush()
            LOGGER.trace(sw.toString())
            val root: RelNode = planner.getRoot()
            if (root != null) {
                root.getCluster().invalidateMetadataQuery()
            }
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * MatchList represents a set of [rule-matches][VolcanoRuleMatch].
     */
    private class MatchList {
        /**
         * Rule match queue for SubstitutionRule.
         */
        val preQueue: Queue<VolcanoRuleMatch> = ArrayDeque()

        /**
         * Current list of VolcanoRuleMatches for this phase. New rule-matches
         * are appended to the end of this queue.
         * The rules are not sorted in any way.
         */
        val queue: Queue<VolcanoRuleMatch> = ArrayDeque()

        /**
         * A set of rule-match names contained in [.queue]. Allows fast
         * detection of duplicate rule-matches.
         */
        val names: Set<String> = HashSet()

        /**
         * Multi-map of RelSubset to VolcanoRuleMatches.
         */
        val matchMap: Multimap<RelSubset, VolcanoRuleMatch> = HashMultimap.create()
        fun size(): Int {
            return preQueue.size() + queue.size()
        }

        @Nullable
        fun poll(): VolcanoRuleMatch? {
            var match: VolcanoRuleMatch = preQueue.poll()
            if (match == null) {
                match = queue.poll()
            }
            return match
        }

        fun offer(match: VolcanoRuleMatch) {
            if (match.getRule() is SubstitutionRule) {
                preQueue.offer(match)
            } else {
                queue.offer(match)
            }
        }

        fun clear() {
            preQueue.clear()
            queue.clear()
            names.clear()
            matchMap.clear()
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val LOGGER: Logger = CalciteTrace.getPlannerTracer()

        /**
         * Dumps rules queue to the logger when debug level is set to `TRACE`.
         */
        private fun dumpRuleQueue(matchList: MatchList) {
            if (LOGGER.isTraceEnabled()) {
                val b = StringBuilder()
                b.append("Rule queue:")
                for (rule in matchList.preQueue) {
                    b.append("\n")
                    b.append(rule)
                }
                for (rule in matchList.queue) {
                    b.append("\n")
                    b.append(rule)
                }
                LOGGER.trace(b.toString())
            }
        }
    }
}
