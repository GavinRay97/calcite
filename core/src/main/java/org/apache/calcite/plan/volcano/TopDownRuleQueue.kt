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
import org.apache.calcite.util.Pair
import java.util.ArrayDeque
import java.util.Deque
import java.util.HashMap
import java.util.HashSet
import java.util.Iterator
import java.util.Map
import java.util.Set
import java.util.function.Predicate

/**
 * A rule queue that manages rule matches for cascades planner.
 */
internal class TopDownRuleQueue(planner: VolcanoPlanner?) : RuleQueue(planner) {
    private val matches: Map<RelNode, Deque<VolcanoRuleMatch>> = HashMap()
    private val names: Set<String> = HashSet()
    @Override
    fun addMatch(match: VolcanoRuleMatch) {
        val rel: RelNode = match.rel(0)
        val queue: Deque<VolcanoRuleMatch> = matches.computeIfAbsent(rel) { id -> ArrayDeque() }
        addMatch(match, queue)
    }

    private fun addMatch(match: VolcanoRuleMatch, queue: Deque<VolcanoRuleMatch>) {
        if (!names.add(match.toString())) {
            return
        }

        // The substitution rule would be applied first though it is added at the end of the queue.
        // The process looks like:
        //   1) put the non-substitution rule at the front and substitution rule at the end of the queue
        //   2) get each rule from the queue in order from first to last and generate an ApplyRule task
        //   3) push each ApplyRule task into the task stack
        // As a result, substitution rule is executed first since the ApplyRule(substitution) task is
        // popped earlier than the ApplyRule(non-substitution) task from the stack.
        if (!planner.isSubstituteRule(match)) {
            queue.addFirst(match)
        } else {
            queue.addLast(match)
        }
    }

    @Nullable
    fun popMatch(category: Pair<RelNode?, Predicate<VolcanoRuleMatch?>?>): VolcanoRuleMatch? {
        val queue: Deque<VolcanoRuleMatch> = matches[category.left] ?: return null
        val iterator: Iterator<VolcanoRuleMatch> = queue.iterator()
        while (iterator.hasNext()) {
            val next: VolcanoRuleMatch = iterator.next()
            if (category.right != null && !category.right.test(next)) {
                continue
            }
            iterator.remove()
            if (!skipMatch(next)) {
                return next
            }
        }
        return null
    }

    @Override
    fun clear(): Boolean {
        val empty = matches.isEmpty()
        matches.clear()
        names.clear()
        return !empty
    }
}
