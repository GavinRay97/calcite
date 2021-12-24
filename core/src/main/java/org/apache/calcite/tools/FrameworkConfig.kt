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

import org.apache.calcite.materialize.SqlStatisticProvider
import org.apache.calcite.plan.Context
import org.apache.calcite.plan.RelOptCostFactory
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitDef
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.rex.RexExecutor
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql2rel.SqlRexConvertletTable
import org.apache.calcite.sql2rel.SqlToRelConverter
import com.google.common.collect.ImmutableList

/**
 * Interface that describes how to configure planning sessions generated
 * using the Frameworks tools.
 *
 * @see Frameworks.newConfigBuilder
 */
interface FrameworkConfig {
    /**
     * The configuration of SQL parser.
     */
    val parserConfig: SqlParser.Config?

    /**
     * The configuration of [SqlValidator].
     */
    val sqlValidatorConfig: SqlValidator.Config?

    /**
     * The configuration of [SqlToRelConverter].
     */
    val sqlToRelConverterConfig: SqlToRelConverter.Config?

    /**
     * Returns the default schema that should be checked before looking at the
     * root schema.  Returns null to only consult the root schema.
     */
    @get:Nullable
    val defaultSchema: SchemaPlus?

    /**
     * Returns the executor used to evaluate constant expressions.
     */
    @get:Nullable
    val executor: RexExecutor?

    /**
     * Returns a list of one or more programs used during the course of query
     * evaluation.
     *
     *
     * The common use case is when there is a single program
     * created using [Programs.of]
     * and [org.apache.calcite.tools.Planner.transform]
     * will only be called once.
     *
     *
     * However, consumers may also create programs
     * not based on rule sets, register multiple programs,
     * and do multiple repetitions
     * of [Planner.transform] planning cycles using different indices.
     *
     *
     * The order of programs provided here determines the zero-based indices
     * of programs elsewhere in this class.
     */
    val programs: ImmutableList<Program?>?

    /**
     * Returns operator table that should be used to
     * resolve functions and operators during query validation.
     */
    val operatorTable: SqlOperatorTable?

    /**
     * Returns the cost factory that should be used when creating the planner.
     * If null, use the default cost factory for that planner.
     */
    @get:Nullable
    val costFactory: RelOptCostFactory?

    /**
     * Returns a list of trait definitions.
     *
     *
     * If the list is not null, the planner first de-registers any
     * existing [RelTraitDef]s, then registers the `RelTraitDef`s in
     * this list.
     *
     *
     * The order of `RelTraitDef`s in the list matters if the
     * planner is VolcanoPlanner. The planner calls [RelTraitDef.convert] in
     * the order of this list. The most important trait comes first in the list,
     * followed by the second most important one, etc.
     */
    @get:Nullable
    val traitDefs: ImmutableList<RelTraitDef?>?

    /**
     * Returns the convertlet table that should be used when converting from SQL
     * to row expressions.
     */
    val convertletTable: SqlRexConvertletTable?

    /**
     * Returns the PlannerContext that should be made available during planning by
     * calling [org.apache.calcite.plan.RelOptPlanner.getContext].
     */
    val context: Context?

    /**
     * Returns the type system.
     */
    val typeSystem: RelDataTypeSystem?

    /**
     * Returns whether the lattice suggester should try to widen a lattice when a
     * new query arrives that doesn't quite fit, as opposed to creating a new
     * lattice.
     */
    val isEvolveLattice: Boolean

    /**
     * Returns the source of statistics about tables and columns to be used
     * by the lattice suggester to deduce primary keys, foreign keys, and the
     * direction of relationships.
     */
    val statisticProvider: SqlStatisticProvider?

    /**
     * Returns a view expander.
     */
    val viewExpander: @Nullable RelOptTable.ViewExpander?
}
