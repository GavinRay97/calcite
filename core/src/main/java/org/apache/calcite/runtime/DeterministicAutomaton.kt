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

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import java.util.HashSet
import java.util.Objects
import java.util.Optional
import java.util.Set
import java.util.stream.Collectors

/**
 * A deterministic finite automaton (DFA).
 *
 *
 * It is constructed from a
 * [nondeterministic finite state automaton (NFA)][Automaton].
 */
class DeterministicAutomaton @SuppressWarnings("method.invocation.invalid") internal constructor(automaton: Automaton) {
    val startState: MultiState
    private val automaton: Automaton
    private val endStates: ImmutableSet<MultiState>
    private val transitions: ImmutableList<Transition>

    /** Constructs the DFA from an epsilon-NFA.  */
    init {
        this.automaton = Objects.requireNonNull(automaton, "automaton")
        // Calculate eps closure of start state
        val traversedStates: Set<MultiState> = HashSet()
        // Add transitions
        startState = epsilonClosure(automaton.startState)
        val transitionsBuilder: ImmutableList.Builder<Transition> = ImmutableList.builder()
        traverse(startState, transitionsBuilder, traversedStates)
        // Store transitions
        transitions = transitionsBuilder.build()

        // Calculate final States
        val endStateBuilder: ImmutableSet.Builder<MultiState> = ImmutableSet.builder()
        traversedStates.stream()
            .filter { ms -> ms.contains(automaton.endState) }
            .forEach(endStateBuilder::add)
        endStates = endStateBuilder.build()
    }

    fun getEndStates(): ImmutableSet<MultiState> {
        return endStates
    }

    fun getTransitions(): ImmutableList<Transition> {
        return transitions
    }

    private fun traverse(
        start: MultiState,
        transitionsBuilder: ImmutableList.Builder<Transition>,
        traversedStates: Set<MultiState>
    ) {
        traversedStates.add(start)
        val newStates: Set<MultiState> = HashSet()
        for (symbol in 0 until automaton.symbolNames.size()) {
            val next: Optional<MultiState> = addTransitions(start, symbol, transitionsBuilder)
            next.ifPresent(newStates::add)
        }
        // Remove all already known states
        newStates.removeAll(traversedStates)
        // If we have really new States, then traverse them
        newStates.forEach { s -> traverse(s, transitionsBuilder, traversedStates) }
    }

    private fun addTransitions(
        start: MultiState, symbol: Int,
        transitionsBuilder: ImmutableList.Builder<Transition>
    ): Optional<MultiState> {
        val builder: ImmutableSet.Builder<Automaton.State> = ImmutableSet.builder()
        for (transition in automaton.getTransitions()) {
            // Consider only transitions for the given symbol
            if (transition.symbol !== symbol) {
                continue
            }
            // Consider only those emitting from current state
            if (!start.contains(transition.fromState)) {
                continue
            }
            // ...
            builder.addAll(epsilonClosure(transition.toState).states)
        }
        val stateSet: ImmutableSet<Automaton.State> = builder.build()
        if (stateSet.isEmpty()) {
            return Optional.empty()
        }
        val next = MultiState(builder.build())
        val transition = Transition(start, next, symbol, automaton.symbolNames.get(symbol))
        // Add the state to the list and add the transition in the table
        transitionsBuilder.add(transition)
        return Optional.of(next)
    }

    private fun epsilonClosure(state: Automaton.State): MultiState {
        val closure: Set<Automaton.State> = HashSet()
        finder(state, closure)
        return MultiState(ImmutableSet.copyOf(closure))
    }

    private fun finder(state: Automaton.State, closure: Set<Automaton.State>) {
        closure.add(state)
        val newStates: Set<Automaton.State> = automaton.getEpsilonTransitions().stream()
            .filter { t -> t.fromState.equals(state) }
            .map { t -> t.toState }
            .collect(Collectors.toSet())
        newStates.removeAll(closure)
        // Recursively call all "new" states
        for (s in newStates) {
            finder(s, closure)
        }
    }

    /** Transition between states.  */
    class Transition(
        fromState: MultiState?, toState: MultiState?, symbolId: Int,
        symbol: String?
    ) {
        val fromState: MultiState
        val toState: MultiState
        val symbolId: Int
        val symbol: String

        init {
            this.fromState = Objects.requireNonNull(fromState, "fromState")
            this.toState = Objects.requireNonNull(toState, "toState")
            this.symbolId = symbolId
            this.symbol = Objects.requireNonNull(symbol, "symbol")
        }
    }

    /**
     * A state of the deterministic finite automaton. Consists of a set of states
     * from the underlying eps-NFA.
     */
    class MultiState(states: ImmutableSet<Automaton.State?>?) {
        val states: ImmutableSet<Automaton.State>

        constructor(vararg states: Automaton.State?) : this(ImmutableSet.copyOf(states)) {}

        init {
            this.states = Objects.requireNonNull(states, "states")
        }

        operator fun contains(state: Automaton.State?): Boolean {
            return states.contains(state)
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || o is MultiState
                    && Objects.equals(states, (o as MultiState).states))
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(states)
        }

        @Override
        override fun toString(): String {
            return states.toString()
        }
    }
}
