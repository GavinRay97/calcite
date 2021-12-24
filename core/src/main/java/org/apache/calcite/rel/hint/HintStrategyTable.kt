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
package org.apache.calcite.rel.hint

import org.apache.calcite.plan.RelOptRule

/**
 * A collection of [HintStrategy]s.
 *
 *
 * Every hint must register a [HintStrategy] into the collection.
 * With a hint strategies mapping, the hint strategy table is used as a tool
 * to decide i) if the given hint was registered; ii) which hints are suitable for the rel with
 * a given hints collection; iii) if the hint options are valid.
 *
 *
 * The hint strategy table is immutable. To create one, use
 * [.builder].
 *
 *
 * Match of hint name is case insensitive.
 *
 * @see HintPredicate
 */
class HintStrategyTable private constructor(strategies: Map<Key, HintStrategy>, litmus: Litmus) {
    //~ Instance fields --------------------------------------------------------
    /** Mapping from hint name to [HintStrategy].  */
    private val strategies: Map<Key, HintStrategy>

    /** Handler for the hint error.  */
    private val errorHandler: Litmus

    init {
        this.strategies = ImmutableMap.copyOf(strategies)
        errorHandler = litmus
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Applies this [HintStrategyTable] hint strategies to the given relational
     * expression and the `hints`.
     *
     * @param hints Hints that may attach to the `rel`
     * @param rel   Relational expression
     * @return A hint list that can be attached to the `rel`
     */
    fun apply(hints: List<RelHint?>, rel: RelNode): List<RelHint> {
        return hints.stream()
            .filter { relHint -> canApply(relHint, rel) }
            .collect(Collectors.toList())
    }

    private fun canApply(hint: RelHint, rel: RelNode): Boolean {
        val key = Key.of(hint.hintName)
        assert(strategies.containsKey(key)) { "hint " + hint.hintName.toString() + " must be present" }
        return strategies[key]!!.predicate.apply(hint, rel)
    }

    /**
     * Checks if the given hint is valid.
     *
     * @param hint The hint
     */
    fun validateHint(hint: RelHint): Boolean {
        val key = Key.of(hint.hintName)
        val hintExists: Boolean = errorHandler.check(
            strategies.containsKey(key),
            "Hint: {} should be registered in the {}",
            hint.hintName,
            this.getClass().getSimpleName()
        )
        if (!hintExists) {
            return false
        }
        val strategy: HintStrategy? = strategies[key]
        return if (strategy != null && strategy.hintOptionChecker != null) {
            strategy.hintOptionChecker.checkOptions(hint, errorHandler)
        } else true
    }

    /** Returns whether the `hintable` has hints that imply
     * the given `rule` should be excluded.  */
    fun isRuleExcluded(hintable: Hintable, rule: RelOptRule?): Boolean {
        val hints: List<RelHint> = hintable.getHints()
        if (hints.size() === 0) {
            return false
        }
        for (hint in hints) {
            val key = Key.of(hint.hintName)
            assert(strategies.containsKey(key)) { "hint " + hint.hintName.toString() + " must be present" }
            val strategy: HintStrategy? = strategies[key]
            if (strategy!!.excludedRules.contains(rule)) {
                return isDesiredConversionPossible(strategy!!.converterRules, hintable)
            }
        }
        return false
    }
    //~ Inner Class ------------------------------------------------------------
    /**
     * Key used to keep the strategies which ignores the case sensitivity.
     */
    private class Key private constructor(private val name: String) {
        @Override
        override fun equals(@Nullable o: Object?): Boolean {
            if (this === o) {
                return true
            }
            if (o == null || getClass() !== o.getClass()) {
                return false
            }
            val key = o as Key
            return name.equals(key.name)
        }

        @Override
        override fun hashCode(): Int {
            return name.hashCode()
        }

        companion object {
            fun of(name: String): Key {
                return Key(name.toLowerCase(Locale.ROOT))
            }
        }
    }

    /**
     * Builder for `HintStrategyTable`.
     */
    class Builder {
        private val strategies: Map<Key, HintStrategy> = HashMap()
        private var errorHandler: Litmus = HintErrorLogger.INSTANCE
        fun hintStrategy(hintName: String, hintPredicate: HintPredicate?): Builder {
            strategies.put(
                Key.of(hintName),
                HintStrategy.builder(requireNonNull(hintPredicate, "hintPredicate")).build()
            )
            return this
        }

        fun hintStrategy(hintName: String, hintStrategy: HintStrategy?): Builder {
            strategies.put(Key.of(hintName), requireNonNull(hintStrategy, "hintStrategy"))
            return this
        }

        /**
         * Sets an error handler to customize the hints error handling.
         *
         *
         * The default behavior is to log warnings.
         *
         * @param errorHandler The handler
         */
        fun errorHandler(errorHandler: Litmus): Builder {
            this.errorHandler = errorHandler
            return this
        }

        fun build(): HintStrategyTable {
            return HintStrategyTable(
                strategies,
                errorHandler
            )
        }
    }

    /** Implementation of [org.apache.calcite.util.Litmus] that returns
     * a status code, it logs warnings for fail check and does not throw.  */
    class HintErrorLogger : Litmus {
        @Override
        fun fail(@Nullable message: String?, @Nullable vararg args: Object?): Boolean {
            LOGGER.warn(requireNonNull(message, "message"), args)
            return false
        }

        @Override
        fun succeed(): Boolean {
            return true
        }

        @Override
        fun check(
            condition: Boolean, @Nullable message: String?,
            @Nullable vararg args: Object?
        ): Boolean {
            return if (condition) {
                succeed()
            } else {
                fail(message, *args)
            }
        }

        companion object {
            private val LOGGER: Logger = CalciteTrace.PARSER_LOGGER
            val INSTANCE = HintErrorLogger()
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /** Empty strategies.  */
        val EMPTY = HintStrategyTable(ImmutableMap.of(), HintErrorLogger.INSTANCE)

        /** Returns whether the `hintable` has hints that imply
         * the given `hintable` can make conversion successfully.  */
        private fun isDesiredConversionPossible(
            converterRules: Set<ConverterRule>,
            hintable: Hintable
        ): Boolean {
            // If no converter rules are specified, we assume the conversion is possible.
            return (converterRules.size() === 0
                    || converterRules.stream()
                .anyMatch { converterRule -> converterRule.convert(hintable as RelNode) != null })
        }

        /**
         * Returns a `HintStrategyTable` builder.
         */
        fun builder(): Builder {
            return Builder()
        }
    }
}
