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
package org.apache.calcite.rel

import org.apache.calcite.plan.RelMultipleTrait

/**
 * Simple implementation of [RelCollation].
 */
class RelCollationImpl(fieldCollations: ImmutableList<RelFieldCollation?>) : RelCollation {
    //~ Instance fields --------------------------------------------------------
    private override val fieldCollations: ImmutableList<RelFieldCollation>

    //~ Constructors -----------------------------------------------------------
    init {
        this.fieldCollations = fieldCollations
        Preconditions.checkArgument(
            Util.isDistinct(RelCollations.ordinals(fieldCollations)),
            "fields must be distinct"
        )
    }

    //~ Methods ----------------------------------------------------------------
    val traitDef: RelTraitDef
        @Override get() = RelCollationTraitDef.INSTANCE

    @Override
    fun getFieldCollations(): List<RelFieldCollation> {
        return fieldCollations
    }

    @Override
    override fun hashCode(): Int {
        return fieldCollations.hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (this === obj) {
            return true
        }
        if (obj is RelCollationImpl) {
            val that = obj as RelCollationImpl
            return fieldCollations.equals(that.fieldCollations)
        }
        return false
    }

    val isTop: Boolean
        @Override get() = fieldCollations.isEmpty()

    @Override
    operator fun compareTo(o: RelMultipleTrait): Int {
        val that = o as RelCollationImpl
        val iterator: UnmodifiableIterator<RelFieldCollation> = that.fieldCollations.iterator()
        for (f in fieldCollations) {
            if (!iterator.hasNext()) {
                return 1
            }
            val f2: RelFieldCollation = iterator.next()
            val c: Int = Utilities.compare(f.getFieldIndex(), f2.getFieldIndex())
            if (c != 0) {
                return c
            }
        }
        return if (iterator.hasNext()) -1 else 0
    }

    @Override
    fun register(planner: RelOptPlanner?) {
    }

    /**
     * Applies mapping to a given collation.
     *
     * If mapping destroys the collation prefix, this method returns an empty collation.
     * Examples of applying mappings to collation [0, 1]:
     *
     *  * mapping(0, 1) =&gt; [0, 1]
     *  * mapping(1, 0) =&gt; [1, 0]
     *  * mapping(0) =&gt; [0]
     *  * mapping(1) =&gt; []
     *  * mapping(2, 0) =&gt; [1]
     *  * mapping(2, 1, 0) =&gt; [2, 1]
     *  * mapping(2, 1) =&gt; []
     *
     *
     * @param mapping   Mapping
     * @return Collation with applied mapping.
     */
    @Override
    fun apply(
        mapping: Mappings.TargetMapping?
    ): RelCollationImpl {
        return RexUtil.apply(mapping, this)
    }

    @Override
    fun satisfies(trait: RelTrait): Boolean {
        return (this === trait
                || trait is RelCollationImpl
                && Util.startsWith(
            fieldCollations,
            (trait as RelCollationImpl).fieldCollations
        ))
    }

    /** Returns a string representation of this collation, suitably terse given
     * that it will appear in plan traces. Examples:
     * "[]", "[2]", "[0 DESC, 1]", "[0 DESC, 1 ASC NULLS LAST]".  */
    @Override
    override fun toString(): String {
        val it: Iterator<RelFieldCollation> = fieldCollations.iterator()
        if (!it.hasNext()) {
            return "[]"
        }
        val sb = StringBuilder()
        sb.append('[')
        while (true) {
            val e: RelFieldCollation = it.next()
            sb.append(e.getFieldIndex())
            if (e.direction !== RelFieldCollation.Direction.ASCENDING
                || e.nullDirection !== e.direction.defaultNullDirection()
            ) {
                sb.append(' ').append(e.shortString())
            }
            if (!it.hasNext()) {
                return sb.append(']').toString()
            }
            sb.append(',').append(' ')
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        @Deprecated // to be removed before 2.0
        val EMPTY: RelCollation = RelCollations.EMPTY

        @Deprecated // to be removed before 2.0
        val PRESERVE: RelCollation = RelCollations.PRESERVE

        @Deprecated // to be removed before 2.0
        fun of(vararg fieldCollations: RelFieldCollation?): RelCollation {
            return RelCollations.of(fieldCollations)
        }

        @Deprecated // to be removed before 2.0
        fun of(fieldCollations: List<RelFieldCollation?>?): RelCollation {
            return RelCollations.of(fieldCollations)
        }

        @Deprecated // to be removed before 2.0
        fun createSingleton(fieldIndex: Int): List<RelCollation> {
            return RelCollations.createSingleton(fieldIndex)
        }

        @Deprecated // to be removed before 2.0
        fun isValid(
            rowType: RelDataType,
            collationList: List<RelCollation>,
            fail: Boolean
        ): Boolean {
            return RelCollations.isValid(rowType, collationList, fail)
        }

        @Deprecated // to be removed before 2.0
        fun equal(
            collationList1: List<RelCollation?>,
            collationList2: List<RelCollation?>?
        ): Boolean {
            return RelCollations.equal(collationList1, collationList2)
        }

        @Deprecated // to be removed before 2.0
        fun ordinals(collation: RelCollation): List<Integer> {
            return RelCollations.ordinals(collation)
        }
    }
}
