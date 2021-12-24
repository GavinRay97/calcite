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
package org.apache.calcite.sql

import org.apache.calcite.sql.dialect.CalciteSqlDialect

/**
 * Specification of a SQL sample.
 *
 *
 * For example, the query
 *
 * <blockquote>
 * <pre>SELECT *
 * FROM emp TABLESAMPLE SUBSTITUTE('medium')</pre>
</blockquote> *
 *
 *
 * declares a sample which is created using [.createNamed].
 *
 *
 * A sample is not a [SqlNode]. To include it in a parse tree, wrap it
 * as a literal, viz:
 * [SqlLiteral.createSample].
 */
object SqlSampleSpec {
    //~ Methods ----------------------------------------------------------------
    /**
     * Creates a sample which substitutes one relation for another.
     */
    fun createNamed(name: String): SqlSampleSpec {
        return SqlSubstitutionSampleSpec(name)
    }

    /**
     * Creates a table sample without repeatability.
     *
     * @param isBernoulli      true if Bernoulli style sampling is to be used;
     * false for implementation specific sampling
     * @param samplePercentage likelihood of a row appearing in the sample
     */
    fun createTableSample(
        isBernoulli: Boolean,
        samplePercentage: Float
    ): SqlSampleSpec {
        return SqlTableSampleSpec(isBernoulli, samplePercentage)
    }

    /**
     * Creates a table sample with repeatability.
     *
     * @param isBernoulli      true if Bernoulli style sampling is to be used;
     * false for implementation specific sampling
     * @param samplePercentage likelihood of a row appearing in the sample
     * @param repeatableSeed   seed value used to reproduce the same sample
     */
    fun createTableSample(
        isBernoulli: Boolean,
        samplePercentage: Float,
        repeatableSeed: Int
    ): SqlSampleSpec {
        return SqlTableSampleSpec(
            isBernoulli,
            samplePercentage,
            repeatableSeed
        )
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Sample specification that orders substitution.  */
    class SqlSubstitutionSampleSpec(val name: String) : SqlSampleSpec() {

        @Override
        override fun toString(): String {
            return ("SUBSTITUTE("
                    + CalciteSqlDialect.DEFAULT.quoteStringLiteral(name)
                    ) + ")"
        }
    }

    /** Sample specification.  */
    class SqlTableSampleSpec : SqlSampleSpec {
        /**
         * Indicates Bernoulli vs. System sampling.
         */
        val isBernoulli: Boolean

        /**
         * Returns sampling percentage. Range is 0.0 to 1.0, exclusive
         */
        val samplePercentage: Float

        /**
         * Indicates whether repeatable seed should be used.
         */
        val isRepeatable: Boolean

        /**
         * Seed to produce repeatable samples.
         */
        val repeatableSeed: Int

        constructor(isBernoulli: Boolean, samplePercentage: Float) {
            this.isBernoulli = isBernoulli
            this.samplePercentage = samplePercentage
            isRepeatable = false
            repeatableSeed = 0
        }

        constructor(
            isBernoulli: Boolean,
            samplePercentage: Float,
            repeatableSeed: Int
        ) {
            this.isBernoulli = isBernoulli
            this.samplePercentage = samplePercentage
            isRepeatable = true
            this.repeatableSeed = repeatableSeed
        }

        @Override
        override fun toString(): String {
            val b = StringBuilder()
            b.append(if (isBernoulli) "BERNOULLI" else "SYSTEM")
            b.append('(')
            b.append(samplePercentage * 100.0)
            b.append(')')
            if (isRepeatable) {
                b.append(" REPEATABLE(")
                b.append(repeatableSeed)
                b.append(')')
            }
            return b.toString()
        }
    }
}
