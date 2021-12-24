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

import org.apache.calcite.rel.core.Match
import org.apache.calcite.util.ImmutableBitSet
import com.google.common.collect.ImmutableList
import java.util.Objects

/** A nondeterministic finite-state automaton (NFA).
 *
 *
 * It is used to implement the [Match]
 * relational expression (for the `MATCH_RECOGNIZE` clause in SQL).
 *
 * @see Pattern
 *
 * @see AutomatonBuilder
 *
 * @see DeterministicAutomaton
 */
class Automaton internal constructor(
    startState: State?, endState: State?,
    transitions: ImmutableList<SymbolTransition?>?,
    epsilonTransitions: ImmutableList<EpsilonTransition?>,
    symbolNames: ImmutableList<String?>?
) {
    val startState: State
    val endState: State
    private val transitions: ImmutableList<SymbolTransition>
    private val epsilonTransitions: ImmutableList<EpsilonTransition>
    val symbolNames: ImmutableList<String>

    /** Use an [AutomatonBuilder].  */
    init {
        this.startState = Objects.requireNonNull(startState, "startState")
        this.endState = Objects.requireNonNull(endState, "endState")
        this.transitions = Objects.requireNonNull(transitions, "transitions")
        this.epsilonTransitions = epsilonTransitions
        this.symbolNames = Objects.requireNonNull(symbolNames, "symbolNames")
    }

    /** Returns the set of states, represented as a bit set, that the graph is
     * in when starting from `state` and receiving `symbol`.
     *
     * @param fromState Initial state
     * @param symbol Symbol received
     * @param bitSet Set of successor states (output)
     */
    fun successors(
        fromState: Int, symbol: Int,
        bitSet: ImmutableBitSet.Builder
    ) {
        for (transition in transitions) {
            if (transition.fromState.id == fromState
                && transition.symbol == symbol
            ) {
                epsilonSuccessors(transition.toState.id, bitSet)
            }
        }
    }

    fun epsilonSuccessors(state: Int, bitSet: ImmutableBitSet.Builder) {
        bitSet.set(state)
        for (transition in epsilonTransitions) {
            if (transition.fromState.id == state) {
                if (!bitSet.get(transition.toState.id)) {
                    epsilonSuccessors(transition.toState.id, bitSet)
                }
            }
        }
    }

    fun getTransitions(): ImmutableList<SymbolTransition> {
        return transitions
    }

    fun getEpsilonTransitions(): ImmutableList<EpsilonTransition> {
        return epsilonTransitions
    }

    /** Node in the finite-state automaton. A state has a number of
     * transitions to other states, each labeled with the symbol that
     * causes that transition.  */
    class State(val id: Int) {
        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (o === this
                    || o is State
                    && (o as State).id == id)
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(id)
        }

        @Override
        override fun toString(): String {
            return ("State{"
                    + "id=" + id
                    + '}')
        }
    }

    /** Transition from one state to another in the finite-state automaton.  */
    internal abstract class Transition(fromState: State?, toState: State?) {
        val fromState: State
        val toState: State

        init {
            this.fromState = Objects.requireNonNull(fromState, "fromState")
            this.toState = Objects.requireNonNull(toState, "toState")
        }
    }

    /** A transition caused by reading a symbol.  */
    class SymbolTransition(fromState: State?, toState: State?, val symbol: Int) : Transition(fromState, toState) {
        @Override
        override fun toString(): String {
            return symbol.toString() + ":" + fromState.id + "->" + toState.id
        }
    }

    /** A transition that may happen without reading a symbol.  */
    class EpsilonTransition(fromState: State?, toState: State?) : Transition(fromState, toState) {
        @Override
        override fun toString(): String {
            return "epsilon:" + fromState.id + "->" + toState.id
        }
    }
}
