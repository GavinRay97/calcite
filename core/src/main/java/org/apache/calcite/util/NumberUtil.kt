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
package org.apache.calcite.util

import org.checkerframework.checker.nullness.qual.PolyNull

/**
 * Utility functions for working with numbers.
 */
object NumberUtil {
    //~ Static fields/initializers ---------------------------------------------
    private val FLOAT_FORMATTER: DecimalFormat? = null
    private val DOUBLE_FORMATTER: DecimalFormat? = null
    private val BIG_INT_TEN_POW: Array<BigInteger?>
    private val BIG_INT_MIN_UNSCALED: Array<BigInteger?>
    private val BIG_INT_MAX_UNSCALED: Array<BigInteger?>

    init {
        // TODO: DecimalFormat uses ROUND_HALF_EVEN, not ROUND_HALF_UP
        // Float: precision of 7 (6 digits after .)
        FLOAT_FORMATTER = decimalFormat("0.######E0")

        // Double: precision of 16 (15 digits after .)
        DOUBLE_FORMATTER = decimalFormat("0.###############E0")
        BIG_INT_TEN_POW = arrayOfNulls<BigInteger>(20)
        BIG_INT_MIN_UNSCALED = arrayOfNulls<BigInteger>(20)
        BIG_INT_MAX_UNSCALED = arrayOfNulls<BigInteger>(20)
        for (i in BIG_INT_TEN_POW.indices) {
            BIG_INT_TEN_POW[i] = BigInteger.TEN.pow(i)
            if (i < 19) {
                BIG_INT_MAX_UNSCALED[i] = BIG_INT_TEN_POW[i].subtract(BigInteger.ONE)
                BIG_INT_MIN_UNSCALED[i] = BIG_INT_MAX_UNSCALED[i].negate()
            } else {
                BIG_INT_MAX_UNSCALED[i] = BigInteger.valueOf(Long.MAX_VALUE)
                BIG_INT_MIN_UNSCALED[i] = BigInteger.valueOf(Long.MIN_VALUE)
            }
        }
    }
    //~ Methods ----------------------------------------------------------------
    /** Creates a format. Locale-independent.  */
    fun decimalFormat(pattern: String?): DecimalFormat {
        return DecimalFormat(
            pattern,
            DecimalFormatSymbols.getInstance(Locale.ROOT)
        )
    }

    fun powTen(exponent: Int): BigInteger? {
        return if (exponent >= 0 && exponent < BIG_INT_TEN_POW.size) {
            BIG_INT_TEN_POW[exponent]
        } else {
            BigInteger.TEN.pow(exponent)
        }
    }

    fun getMaxUnscaled(precision: Int): BigInteger? {
        return BIG_INT_MAX_UNSCALED[precision]
    }

    fun getMinUnscaled(precision: Int): BigInteger? {
        return BIG_INT_MIN_UNSCALED[precision]
    }

    /** Sets the scale of a BigDecimal `bd` if it is not null;
     * always returns `bd`.  */
    @PolyNull
    fun rescaleBigDecimal(
        @PolyNull bd: BigDecimal?,
        scale: Int
    ): BigDecimal? {
        var bd: BigDecimal? = bd
        if (bd != null) {
            bd = bd.setScale(scale, RoundingMode.HALF_UP)
        }
        return bd
    }

    fun toBigDecimal(number: Number?, scale: Int): BigDecimal? {
        val bd: BigDecimal = toBigDecimal(number)
        return rescaleBigDecimal(bd, scale)
    }

    /** Converts a number to a BigDecimal with the same value;
     * returns null if and only if the number is null.  */
    @PolyNull
    fun toBigDecimal(@PolyNull number: Number?): BigDecimal {
        if (number == null) {
            return castNonNull(null)
        }
        return if (number is BigDecimal) {
            number as BigDecimal
        } else if (number is Double
            || number is Float
        ) {
            BigDecimal.valueOf(number.doubleValue())
        } else if (number is BigInteger) {
            BigDecimal(number as BigInteger?)
        } else {
            BigDecimal(number.longValue())
        }
    }

    /** Returns whether a [BigDecimal] is a valid Farrago decimal. If a
     * BigDecimal's unscaled value overflows a long, then it is not a valid
     * Farrago decimal.  */
    fun isValidDecimal(bd: BigDecimal): Boolean {
        val usv: BigInteger = bd.unscaledValue()
        val usvl: Long = usv.longValue()
        return usv.equals(BigInteger.valueOf(usvl))
    }

    fun getApproxFormatter(isFloat: Boolean): NumberFormat? {
        return if (isFloat) FLOAT_FORMATTER else DOUBLE_FORMATTER
    }

    fun round(d: Double): Long {
        return if (d < 0) {
            (d - 0.5).toLong()
        } else {
            (d + 0.5).toLong()
        }
    }

    /** Returns the sum of two numbers, or null if either is null.  */
    @PolyNull
    fun add(@PolyNull a: Double?, @PolyNull b: Double?): Double? {
        return if (a == null || b == null) {
            null
        } else a + b
    }

    /** Returns the difference of two numbers,
     * or null if either is null.  */
    @PolyNull
    fun subtract(@PolyNull a: Double?, @PolyNull b: Double?): Double {
        return if (a == null || b == null) {
            castNonNull(null)
        } else a - b
    }

    /** Returns the quotient of two numbers,
     * or null if either is null or the divisor is zero.  */
    @Nullable
    fun divide(@Nullable a: Double?, @Nullable b: Double?): Double {
        return if (a == null || b == null || b == 0.0) {
            castNonNull(null)
        } else a / b
    }

    /** Returns the product of two numbers,
     * or null if either is null.  */
    @PolyNull
    fun multiply(@PolyNull a: Double?, @PolyNull b: Double?): Double {
        return if (a == null || b == null) {
            castNonNull(null)
        } else a * b
    }

    /** Like [Math.min] but null safe;
     * returns the lesser of two numbers,
     * ignoring numbers that are null,
     * or null if both are null.  */
    @PolyNull
    fun min(@PolyNull a: Double?, @PolyNull b: Double?): Double? {
        return if (a == null) {
            b
        } else if (b == null) {
            a
        } else {
            Math.min(a, b)
        }
    }

    /** Like [Math.max] but null safe;
     * returns the greater of two numbers,
     * or null if either is null.  */
    @PolyNull
    fun max(@PolyNull a: Double?, @PolyNull b: Double?): Double {
        return if (a == null || b == null) {
            castNonNull(null)
        } else Math.max(a, b)
    }
}
