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
package org.apache.calcite.rel.metadata

import org.apache.calcite.plan.RelOptTable

/**
 * RelColumnOrigin is a data structure describing one of the origins of an
 * output column produced by a relational expression.
 */
class RelColumnOrigin(
    originTable: RelOptTable,
    iOriginColumn: Int,
    isDerived: Boolean
) {
    //~ Instance fields --------------------------------------------------------
    private val originTable: RelOptTable

    /** Returns the 0-based index of column in origin table; whether this ordinal
     * is flattened or unflattened depends on whether UDT flattening has already
     * been performed on the relational expression which produced this
     * description.  */
    val originColumnOrdinal: Int

    /**
     * Consider the query `select a+b as c, d as e from t`. The
     * output column c has two origins (a and b), both of them derived. The
     * output column d as one origin (c), which is not derived.
     *
     * @return false if value taken directly from column in origin table; true
     * otherwise
     */
    val isDerived: Boolean

    //~ Constructors -----------------------------------------------------------
    init {
        this.originTable = originTable
        originColumnOrdinal = iOriginColumn
        this.isDerived = isDerived
    }
    //~ Methods ----------------------------------------------------------------
    /** Returns table of origin.  */
    fun getOriginTable(): RelOptTable {
        return originTable
    }

    // override Object
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (obj !is RelColumnOrigin) {
            return false
        }
        val other = obj as RelColumnOrigin
        return (originTable.getQualifiedName().equals(
            other.originTable.getQualifiedName()
        )
                && originColumnOrdinal == other.originColumnOrdinal
                && isDerived == other.isDerived)
    }

    // override Object
    @Override
    override fun hashCode(): Int {
        return (originTable.getQualifiedName().hashCode()
                + originColumnOrdinal + if (isDerived) 313 else 0)
    }
}
