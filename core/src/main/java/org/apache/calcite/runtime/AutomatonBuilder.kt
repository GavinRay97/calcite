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
package org.apache.calcite.runtime

import org.apache.calcite.runtime.Automaton.EpsilonTransition
import org.apache.calcite.runtime.Automaton.State
import org.apache.calcite.runtime.Automaton.SymbolTransition
import org.apache.calcite.runtime.Automaton.Transition
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import java.util.ArrayList
import java.util.Comparator
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Objects

/** Builds a state-transition graph for deterministic finite automaton.  */
class AutomatonBuilder {
    private val symbolIds: Map<String, Integer> = HashMap()
    private val stateList: List<State> = ArrayList()
    private val transitionList: List<Transition> = ArrayList()

    @SuppressWarnings("method.invocation.invalid")
    private val startState: State = createState()

    @SuppressWarnings("method.invocation.invalid")
    private val endState: State = createState()

    /** Adds a pattern as a start-to-end transition.  */
    fun add(pattern: Pattern): AutomatonBuilder {
        return add(pattern, startState, endState)
    }

    private fun add(
        pattern: Pattern, fromState: State,
        toState: State
    ): AutomatonBuilder {
        val p: Pattern.AbstractPattern = pattern as Pattern.AbstractPattern
        return when (p.op) {
            SEQ -> {
                val pSeq: Pattern.OpPattern = p as Pattern.OpPattern
                seq(fromState, toState, pSeq.patterns)
            }
            STAR -> {
                val pStar: Pattern.OpPattern = p as Pattern.OpPattern
                star(fromState, toState, pStar.patterns.get(0))
            }
            PLUS -> {
                val pPlus: Pattern.OpPattern = p as Pattern.OpPattern
                plus(fromState, toState, pPlus.patterns.get(0))
            }
            REPEAT -> {
                val pRepeat: Pattern.RepeatPattern = p as Pattern.RepeatPattern
                repeat(
                    fromState, toState, pRepeat.patterns.get(0),
                    pRepeat.minRepeat, pRepeat.maxRepeat
                )
            }
            SYMBOL -> {
                val pSymbol: Pattern.SymbolPattern = p as Pattern.SymbolPattern
                symbol(fromState, toState, pSymbol.name)
            }
            OR -> {
                val pOr: Pattern.OpPattern = p as Pattern.OpPattern
                or(fromState, toState, pOr.patterns.get(0), pOr.patterns.get(1))
            }
            OPTIONAL -> {
                // Rewrite as {0,1}
                val pOptional: Pattern.OpPattern = p as Pattern.OpPattern
                optional(fromState, toState, pOptional.patterns.get(0))
            }
            else -> throw AssertionError("unknown op " + p.op)
        }
    }

    private fun createState(): State {
        val state = State(stateList.size())
        stateList.add(state)
        return state
    }

    /** Builds the automaton.  */
    fun build(): Automaton {
        val symbolTransitions: ImmutableList.Builder<SymbolTransition> = ImmutableList.builder()
        val epsilonTransitions: ImmutableList.Builder<EpsilonTransition> = ImmutableList.builder()
        for (transition in transitionList) {
            if (transition is SymbolTransition) {
                symbolTransitions.add(transition)
            }
            if (transition is EpsilonTransition) {
                epsilonTransitions.add(transition)
            }
        }
        // Sort the symbols by their ids, which are assumed consecutive and
        // starting from zero.
        val symbolNames: ImmutableList<String?> = symbolIds.entrySet()
            .stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .collect(Util.toImmutableList())
        return Automaton(
            stateList[0], endState,
            symbolTransitions.build(), epsilonTransitions.build(), symbolNames
        )
    }

    /** Adds a symbol transition.  */
    fun symbol(
        fromState: State?, toState: State?,
        name: String?
    ): AutomatonBuilder {
        Objects.requireNonNull(name, "name")
        val symbolId: Int = symbolIds.computeIfAbsent(name) { k -> symbolIds.size() }
        transitionList.add(SymbolTransition(fromState, toState, symbolId))
        return this
    }

    /** Adds a transition made up of a sequence of patterns.  */
    fun seq(
        fromState: State, toState: State,
        patterns: List<Pattern>
    ): AutomatonBuilder {
        var prevState: State = fromState
        for (i in 0 until patterns.size()) {
            val pattern: Pattern = patterns[i]
            val nextState: State
            nextState = if (i == patterns.size() - 1) {
                toState
            } else {
                createState()
            }
            add(pattern, prevState, nextState)
            prevState = nextState
        }
        return this
    }

    /** Adds a transition for the 'or' pattern.  */
    fun or(
        fromState: State, toState: State, left: Pattern,
        right: Pattern
    ): AutomatonBuilder {
        //
        //             left
        //         / -------->  toState
        //  fromState
        //         \ --------> toState
        //             right
        add(left, fromState, toState)
        add(right, fromState, toState)
        return this
    }

    /** Adds a transition made up of the Kleene star applied to a pattern.  */
    fun star(fromState: State?, toState: State?, pattern: Pattern): AutomatonBuilder {
        //
        //     +------------------------- e ------------------------+
        //     |                +-------- e -------+                |
        //     |                |                  |                |
        //     |                V                  |                V
        // fromState -----> beforeState -----> afterState -----> toState
        //             e               pattern              e
        //
        val beforeState: State = createState()
        val afterState: State = createState()
        transitionList.add(EpsilonTransition(fromState, beforeState))
        add(pattern, beforeState, afterState)
        transitionList.add(EpsilonTransition(afterState, beforeState))
        transitionList.add(EpsilonTransition(afterState, toState))
        transitionList.add(EpsilonTransition(fromState, toState))
        return this
    }

    /** Adds a transition made up of a pattern repeated 1 or more times.  */
    fun plus(fromState: State?, toState: State?, pattern: Pattern): AutomatonBuilder {
        //
        //                      +-------- e -------+
        //                      |                  |
        //                      V                  |
        // fromState -----> beforeState -----> afterState -----> toState
        //             e               pattern              e
        //
        val beforeState: State = createState()
        val afterState: State = createState()
        transitionList.add(EpsilonTransition(fromState, beforeState))
        add(pattern, beforeState, afterState)
        transitionList.add(EpsilonTransition(afterState, beforeState))
        transitionList.add(EpsilonTransition(afterState, toState))
        return this
    }

    /** Adds a transition made up of a pattern repeated between `minRepeat`
     * and `maxRepeat` times.  */
    fun repeat(
        fromState: State, toState: State?, pattern: Pattern,
        minRepeat: Int, maxRepeat: Int
    ): AutomatonBuilder {
        // Diagram for repeat(1, 3)
        //                              +-------- e --------------+
        //                              |           +---- e ----+ |
        //                              |           |           V V
        // fromState ---> state0 ---> state1 ---> state2 ---> state3 ---> toState
        //            e        pattern     pattern     pattern        e
        //
        Preconditions.checkArgument(0 <= minRepeat)
        Preconditions.checkArgument(minRepeat <= maxRepeat)
        Preconditions.checkArgument(1 <= maxRepeat)
        var prevState: State = fromState
        for (i in 0..maxRepeat) {
            val s: State = createState()
            if (i == 0) {
                transitionList.add(EpsilonTransition(fromState, s))
            } else {
                add(pattern, prevState, s)
            }
            if (i >= minRepeat) {
                transitionList.add(EpsilonTransition(s, toState))
            }
            prevState = s
        }
        transitionList.add(EpsilonTransition(prevState, toState))
        return this
    }

    private fun optional(fromState: State, toState: State, pattern: Pattern): AutomatonBuilder {
        add(pattern, fromState, toState)
        transitionList.add(EpsilonTransition(fromState, toState))
        return this
    }
}
