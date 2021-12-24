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

import com.google.common.base.Preconditions

/**
 * Hint attached to a relation expression.
 *
 *
 * A hint can be used to:
 *
 *
 *  * Enforce planner: there's no perfect planner, so it makes sense to implement hints to
 * allow user better control the execution. For instance, "never merge this subquery with others",
 * "treat those tables as leading ones" in the join ordering, etc.
 *  * Append meta data/statistics: Some statistics like “table index for scan” and
 * “skew info of some shuffle keys” are somewhat dynamic for the query, it would be very
 * convenient to config them with hints because our planning metadata from the planner is very
 * often not that accurate.
 *  * Operator resource constraints: For many cases, we would give a default resource
 * configuration for the execution operators, i.e. min parallelism or
 * managed memory (resource consuming UDF) or special resource requirement (GPU or SSD disk)
 * and so on, it would be very flexible to profile the resource with hints per query
 * (instead of the Job).
 *
 *
 *
 * In order to support hint override, each hint has a `inheritPath` (integers list) to
 * record its propagate path from the root node, number `0` represents the hint was propagated
 * along the first(left) child, number `1` represents the hint was propagated along the
 * second(right) child. Given a relational expression tree with initial attached hints:
 *
 * <blockquote><pre>
 * Filter (Hint1)
 * |
 * Join
 * /    \
 * Scan  Project (Hint2)
 * |
 * Scan2
</pre></blockquote> *
 *
 *
 * The plan would have hints path as follows (assumes each hint can be propagated to all
 * child nodes):
 *
 * <blockquote>
 *  * Filter &#8594; {Hint1[]}
 *  * Join &#8594; {Hint1[0]}
 *  * Scan &#8594; {Hint1[0, 0]}
 *  * Project &#8594; {Hint1[0,1], Hint2[]}
 *  * Scan2 &#8594; {[Hint1[0, 1, 0], Hint2[0]}
</blockquote> *
 *
 *
 * `listOptions` and `kvOptions` are supposed to contain the same information,
 * they are mutually exclusive, that means, they can not both be non-empty.
 *
 *
 * RelHint is immutable.
 */
class RelHint private constructor(
    inheritPath: Iterable<Integer>,
    hintName: String,
    @Nullable listOption: List<String>?,
    @Nullable kvOptions: Map<String, String>?
) {
    //~ Instance fields --------------------------------------------------------
    val inheritPath: ImmutableList<Integer>
    val hintName: String
    val listOptions: List<String>
    val kvOptions: Map<String, String>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `RelHint`.
     *
     * @param inheritPath Hint inherit path
     * @param hintName    Hint name
     * @param listOption  Hint options as string list
     * @param kvOptions   Hint options as string key value pair
     */
    init {
        Objects.requireNonNull(inheritPath, "inheritPath")
        Objects.requireNonNull(hintName, "hintName")
        this.inheritPath = ImmutableList.copyOf(inheritPath)
        this.hintName = hintName
        listOptions = if (listOption == null) ImmutableList.of() else ImmutableList.copyOf(listOption)
        this.kvOptions = if (kvOptions == null) ImmutableMap.of() else ImmutableMap.copyOf(kvOptions)
    }

    /**
     * Returns a copy of this hint with specified inherit path.
     *
     * @param inheritPath Hint path
     * @return the new `RelHint`
     */
    fun copy(inheritPath: List<Integer>): RelHint {
        Objects.requireNonNull(inheritPath, "inheritPath")
        return RelHint(inheritPath, hintName, listOptions, kvOptions)
    }

    @Override
    override fun equals(@Nullable o: Object?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || getClass() !== o.getClass()) {
            return false
        }
        val hint = o as RelHint
        return (inheritPath.equals(hint.inheritPath)
                && hintName.equals(hint.hintName)
                && Objects.equals(listOptions, hint.listOptions)
                && Objects.equals(kvOptions, hint.kvOptions))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            hintName, inheritPath,
            listOptions, kvOptions
        )
    }

    @Override
    override fun toString(): String {
        val builder = StringBuilder()
        builder.append("[")
            .append(hintName)
            .append(" inheritPath:")
            .append(inheritPath)
        if (listOptions.size() > 0 || kvOptions.size() > 0) {
            builder.append(" options:")
            if (listOptions.size() > 0) {
                builder.append(listOptions.toString())
            } else {
                builder.append(kvOptions.toString())
            }
        }
        builder.append("]")
        return builder.toString()
    }
    //~ Inner Class ------------------------------------------------------------
    /** Builder for [RelHint].  */
    class Builder(hintName: String) {
        private val hintName: String
        private var inheritPath: List<Integer>
        private var listOptions: List<String>
        private var kvOptions: Map<String, String>

        init {
            listOptions = ArrayList()
            kvOptions = LinkedHashMap()
            this.hintName = hintName
            inheritPath = ImmutableList.of()
        }

        /** Sets up the inherit path with given integer list.  */
        fun inheritPath(inheritPath: Iterable<Integer?>?): Builder {
            this.inheritPath = ImmutableList.copyOf(Objects.requireNonNull(inheritPath, "inheritPath"))
            return this
        }

        /** Sets up the inherit path with given integer array.  */
        fun inheritPath(vararg inheritPath: Integer?): Builder {
            this.inheritPath = Arrays.asList(inheritPath)
            return this
        }

        /** Add a hint option as string.  */
        fun hintOption(hintOption: String?): Builder {
            Objects.requireNonNull(hintOption, "hintOption")
            Preconditions.checkState(
                kvOptions.size() === 0,
                "List options and key value options can not be mixed in"
            )
            listOptions.add(hintOption)
            return this
        }

        /** Add multiple string hint options.  */
        fun hintOptions(hintOptions: Iterable<String?>?): Builder {
            Objects.requireNonNull(hintOptions, "hintOptions")
            Preconditions.checkState(
                kvOptions.size() === 0,
                "List options and key value options can not be mixed in"
            )
            listOptions = ImmutableList.copyOf(hintOptions)
            return this
        }

        /** Add a hint option as string key-value pair.  */
        fun hintOption(optionKey: String?, optionValue: String?): Builder {
            Objects.requireNonNull(optionKey, "optionKey")
            Objects.requireNonNull(optionValue, "optionValue")
            Preconditions.checkState(
                listOptions.size() === 0,
                "List options and key value options can not be mixed in"
            )
            kvOptions.put(optionKey, optionValue)
            return this
        }

        /** Add multiple string key-value pair hint options.  */
        fun hintOptions(kvOptions: Map<String, String>): Builder {
            Objects.requireNonNull(kvOptions, "kvOptions")
            Preconditions.checkState(
                listOptions.size() === 0,
                "List options and key value options can not be mixed in"
            )
            this.kvOptions = kvOptions
            return this
        }

        fun build(): RelHint {
            return RelHint(inheritPath, hintName, listOptions, kvOptions)
        }
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        /** Creates a hint builder with specified hint name.  */
        fun builder(hintName: String): Builder {
            return Builder(hintName)
        }
    }
}
