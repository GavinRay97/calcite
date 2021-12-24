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

import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptCostFactory
import org.apache.calcite.plan.RelOptUtil
import java.util.Objects

/**
 * `VolcanoCost` represents the cost of a plan node.
 *
 *
 * This class is immutable: none of the methods modify any member
 * variables.
 */
internal class VolcanoCost     //~ Constructors -----------------------------------------------------------
    (
    @get:Override val rows: Double, //~ Methods ----------------------------------------------------------------
    //~ Instance fields --------------------------------------------------------
    @get:Override val cpu: Double, @get:Override val io: Double
) : RelOptCost {

    @get:Override
    val isInfinite: Boolean
        get() = (this === INFINITY
                || rows == Double.POSITIVE_INFINITY
                || cpu == Double.POSITIVE_INFINITY
                || io == Double.POSITIVE_INFINITY)

    @Override
    fun isLe(other: RelOptCost): Boolean {
        val that = other as VolcanoCost
        return if (true) {
            (this === that
                    || rows <= that.rows)
        } else this === that
                || (rows <= that.rows
                && cpu <= that.cpu
                && io <= that.io)
    }

    @Override
    fun isLt(other: RelOptCost): Boolean {
        if (true) {
            val that = other as VolcanoCost
            return rows < that.rows
        }
        return isLe(other) && !equals(other)
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(rows, cpu, io)
    }

    @SuppressWarnings("NonOverridingEquals")
    @Override
    override fun equals(other: RelOptCost): Boolean {
        return (this === other
                || (other is VolcanoCost
                && rows == (other as VolcanoCost).rows
                && cpu == (other as VolcanoCost).cpu
                && io == (other as VolcanoCost).io))
    }

    @Override
    override fun equals(@Nullable obj: Object?): Boolean {
        return if (obj is VolcanoCost) {
            equals(obj as VolcanoCost?)
        } else false
    }

    @Override
    fun isEqWithEpsilon(other: RelOptCost): Boolean {
        if (other !is VolcanoCost) {
            return false
        }
        val that = other as VolcanoCost
        return (this === that
                || (Math.abs(rows - that.rows) < RelOptUtil.EPSILON
                && Math.abs(cpu - that.cpu) < RelOptUtil.EPSILON
                && Math.abs(io - that.io) < RelOptUtil.EPSILON))
    }

    @Override
    operator fun minus(other: RelOptCost): RelOptCost {
        if (this === INFINITY) {
            return this
        }
        val that = other as VolcanoCost
        return VolcanoCost(
            rows - that.rows,
            cpu - that.cpu,
            io - that.io
        )
    }

    @Override
    fun multiplyBy(factor: Double): RelOptCost {
        return if (this === INFINITY) {
            this
        } else VolcanoCost(rows * factor, cpu * factor, io * factor)
    }

    @Override
    fun divideBy(cost: RelOptCost): Double {
        // Compute the geometric average of the ratios of all of the factors
        // which are non-zero and finite.
        val that = cost as VolcanoCost
        var d = 1.0
        var n = 0.0
        if (rows != 0.0
            && !Double.isInfinite(rows)
            && that.rows != 0.0
            && !Double.isInfinite(that.rows)
        ) {
            d *= rows / that.rows
            ++n
        }
        if (cpu != 0.0
            && !Double.isInfinite(cpu)
            && that.cpu != 0.0
            && !Double.isInfinite(that.cpu)
        ) {
            d *= cpu / that.cpu
            ++n
        }
        if (io != 0.0
            && !Double.isInfinite(io)
            && that.io != 0.0
            && !Double.isInfinite(that.io)
        ) {
            d *= io / that.io
            ++n
        }
        return if (n == 0.0) {
            1.0
        } else Math.pow(d, 1 / n)
    }

    @Override
    operator fun plus(other: RelOptCost): RelOptCost {
        val that = other as VolcanoCost
        return if (this === INFINITY || that === INFINITY) {
            INFINITY
        } else VolcanoCost(
            rows + that.rows,
            cpu + that.cpu,
            io + that.io
        )
    }

    @Override
    override fun toString(): String {
        return "{" + rows + " rows, " + cpu + " cpu, " + io + " io}"
    }

    /** Implementation of [org.apache.calcite.plan.RelOptCostFactory]
     * that creates [org.apache.calcite.plan.volcano.VolcanoCost]s.  */
    private class Factory : RelOptCostFactory {
        @Override
        fun makeCost(dRows: Double, dCpu: Double, dIo: Double): RelOptCost {
            return VolcanoCost(dRows, dCpu, dIo)
        }

        @Override
        fun makeHugeCost(): RelOptCost {
            return HUGE
        }

        @Override
        fun makeInfiniteCost(): RelOptCost {
            return INFINITY
        }

        @Override
        fun makeTinyCost(): RelOptCost {
            return TINY
        }

        @Override
        fun makeZeroCost(): RelOptCost {
            return ZERO
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val INFINITY: VolcanoCost = object : VolcanoCost(
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY
        ) {
            @Override
            override fun toString(): String {
                return "{inf}"
            }
        }
        val HUGE: VolcanoCost = object : VolcanoCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
            @Override
            override fun toString(): String {
                return "{huge}"
            }
        }
        val ZERO: VolcanoCost = object : VolcanoCost(0.0, 0.0, 0.0) {
            @Override
            override fun toString(): String {
                return "{0}"
            }
        }
        val TINY: VolcanoCost = object : VolcanoCost(1.0, 1.0, 0.0) {
            @Override
            override fun toString(): String {
                return "{tiny}"
            }
        }
        val FACTORY: RelOptCostFactory = Factory()
    }
}
