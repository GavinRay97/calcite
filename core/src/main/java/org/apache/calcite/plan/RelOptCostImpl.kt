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

/**
 * RelOptCostImpl provides a default implementation for the [RelOptCost]
 * interface. It it defined in terms of a single scalar quantity; somewhat
 * arbitrarily, it returns this scalar for rows processed and zero for both CPU
 * and I/O.
 */
class RelOptCostImpl     //~ Constructors -----------------------------------------------------------
    (  //~ Methods ----------------------------------------------------------------
    // implement RelOptCost
    //~ Instance fields --------------------------------------------------------
    @get:Override override val rows: Double
) : RelOptCost {

    // implement RelOptCost
    @get:Override
    override val io: Double
        get() = 0

    // implement RelOptCost
    @get:Override
    override val cpu: Double
        get() = 0

    // implement RelOptCost
    @get:Override
    override val isInfinite: Boolean
        get() = Double.isInfinite(rows)

    // implement RelOptCost
    @Override
    fun isLe(other: RelOptCost): Boolean {
        return rows <= other.getRows()
    }

    // implement RelOptCost
    @Override
    fun isLt(other: RelOptCost): Boolean {
        return rows < other.getRows()
    }

    @Override
    override fun hashCode(): Int {
        return Double.hashCode(rows)
    }

    // implement RelOptCost
    @SuppressWarnings("NonOverridingEquals")
    @Override
    fun equals(other: RelOptCost): Boolean {
        return rows == other.getRows()
    }

    @Override
    override fun equals(@Nullable obj: Object?): Boolean {
        return if (obj is RelOptCostImpl) {
            equals(obj as RelOptCost?)
        } else false
    }

    // implement RelOptCost
    @Override
    fun isEqWithEpsilon(other: RelOptCost): Boolean {
        return Math.abs(rows - other.getRows()) < RelOptUtil.EPSILON
    }

    // implement RelOptCost
    @Override
    operator fun minus(other: RelOptCost): RelOptCost {
        return RelOptCostImpl(rows - other.getRows())
    }

    // implement RelOptCost
    @Override
    operator fun plus(other: RelOptCost): RelOptCost {
        return RelOptCostImpl(rows + other.getRows())
    }

    // implement RelOptCost
    @Override
    override fun multiplyBy(factor: Double): RelOptCost {
        return RelOptCostImpl(rows * factor)
    }

    @Override
    fun divideBy(cost: RelOptCost): Double {
        val that = cost as RelOptCostImpl
        return rows / that.rows
    }

    // implement RelOptCost
    @Override
    override fun toString(): String {
        return if (rows == Double.MAX_VALUE) {
            "huge"
        } else {
            Double.toString(rows)
        }
    }

    /** Implementation of [RelOptCostFactory] that creates
     * [RelOptCostImpl]s.  */
    private class Factory : RelOptCostFactory {
        // implement RelOptPlanner
        @Override
        override fun makeCost(
            dRows: Double,
            dCpu: Double,
            dIo: Double
        ): RelOptCost {
            return RelOptCostImpl(dRows)
        }

        // implement RelOptPlanner
        @Override
        override fun makeHugeCost(): RelOptCost {
            return RelOptCostImpl(Double.MAX_VALUE)
        }

        // implement RelOptPlanner
        @Override
        override fun makeInfiniteCost(): RelOptCost {
            return RelOptCostImpl(Double.POSITIVE_INFINITY)
        }

        // implement RelOptPlanner
        @Override
        override fun makeTinyCost(): RelOptCost {
            return RelOptCostImpl(1.0)
        }

        // implement RelOptPlanner
        @Override
        override fun makeZeroCost(): RelOptCost {
            return RelOptCostImpl(0.0)
        }
    }

    companion object {
        val FACTORY: RelOptCostFactory = Factory()
    }
}
