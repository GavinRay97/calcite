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

import org.apache.calcite.rel.core.TableFunctionScan

/**
 * Visitor that has methods for the common logical relational expressions.
 */
interface RelShuttle {
    fun visit(scan: TableScan?): RelNode?
    fun visit(scan: TableFunctionScan?): RelNode?
    fun visit(values: LogicalValues?): RelNode?
    fun visit(filter: LogicalFilter?): RelNode?
    fun visit(calc: LogicalCalc?): RelNode?
    fun visit(project: LogicalProject?): RelNode?
    fun visit(join: LogicalJoin?): RelNode?
    fun visit(correlate: LogicalCorrelate?): RelNode?
    fun visit(union: LogicalUnion?): RelNode?
    fun visit(intersect: LogicalIntersect?): RelNode?
    fun visit(minus: LogicalMinus?): RelNode?
    fun visit(aggregate: LogicalAggregate?): RelNode?
    fun visit(match: LogicalMatch?): RelNode?
    fun visit(sort: LogicalSort?): RelNode?
    fun visit(exchange: LogicalExchange?): RelNode?
    fun visit(modify: LogicalTableModify?): RelNode?
    fun visit(other: RelNode?): RelNode?
}
