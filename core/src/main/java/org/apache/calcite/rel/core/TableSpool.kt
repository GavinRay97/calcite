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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.function.Experimental

/**
 * Spool that writes into a table.
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
abstract class TableSpool protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, readType: Type?, writeType: Type?, table: RelOptTable?
) : Spool(cluster, traitSet, input, readType, writeType) {
    protected val table: RelOptTable

    init {
        this.table = Objects.requireNonNull(table, "table")
    }

    @Override
    fun getTable(): RelOptTable {
        return table
    }

    @Override
    override fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        return pw.item("table", table.getQualifiedName())
    }
}
