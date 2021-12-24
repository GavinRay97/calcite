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
package org.apache.calcite.tools

import org.apache.calcite.interpreter.Bindables

/** Implementations of [RelRunner].  */
object RelRunners {
    /** Runs a relational expression by creating a JDBC connection.  */
    fun run(rel: RelNode): PreparedStatement? {
        var rel: RelNode = rel
        val shuttle: RelShuttle = object : RelHomogeneousShuttle() {
            @Override
            fun visit(scan: TableScan): RelNode {
                val table: RelOptTable = scan.getTable()
                return if (scan is LogicalTableScan
                    && Bindables.BindableTableScan.canHandle(table)
                ) {
                    // Always replace the LogicalTableScan with BindableTableScan
                    // because it's implementation does not require a "schema" as context.
                    Bindables.BindableTableScan.create(scan.getCluster(), table)
                } else super.visit(scan)
            }
        }
        rel = rel.accept(shuttle)
        try {
            DriverManager.getConnection("jdbc:calcite:").use { connection ->
                val runner: RelRunner = connection.unwrap(RelRunner::class.java)
                return runner.prepareStatement(rel)
            }
        } catch (e: SQLException) {
            throw Util.throwAsRuntime(e)
        }
    }
}
