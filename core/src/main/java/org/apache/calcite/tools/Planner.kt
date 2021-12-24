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

import org.apache.calcite.plan.RelTraitSet

/**
 * A faade that covers Calcite's query planning process: parse SQL,
 * validate the parse tree, convert the parse tree to a relational expression,
 * and optimize the relational expression.
 *
 *
 * Planner is NOT thread safe. However, it can be reused for
 * different queries. The consumer of this interface is responsible for calling
 * reset() after each use of Planner that corresponds to a different
 * query.
 */
interface Planner : AutoCloseable {
    /**
     * Parses and validates a SQL statement.
     *
     * @param sql The SQL statement to parse.
     * @return The root node of the SQL parse tree.
     * @throws org.apache.calcite.sql.parser.SqlParseException on parse error
     */
    @Throws(SqlParseException::class)
    fun parse(sql: String?): SqlNode? {
        return parse(SourceStringReader(sql))
    }

    /**
     * Parses and validates a SQL statement.
     *
     * @param source A reader which will provide the SQL statement to parse.
     *
     * @return The root node of the SQL parse tree.
     * @throws org.apache.calcite.sql.parser.SqlParseException on parse error
     */
    @Throws(SqlParseException::class)
    fun parse(source: Reader?): SqlNode?

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node
     * @throws ValidationException if not valid
     */
    @Throws(ValidationException::class)
    fun validate(sqlNode: SqlNode?): SqlNode?

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node and its validated type.
     * @throws ValidationException if not valid
     */
    @Throws(ValidationException::class)
    fun validateAndGetType(sqlNode: SqlNode?): Pair<SqlNode?, RelDataType?>?

    /**
     * Converts a SQL parse tree into a tree of relational expressions.
     *
     *
     * You must call [.validate] first.
     *
     * @param sql The root node of the SQL parse tree.
     * @return The root node of the newly generated RelNode tree.
     * @throws org.apache.calcite.tools.RelConversionException if the node
     * cannot be converted or has not been validated
     */
    @Throws(RelConversionException::class)
    fun rel(sql: SqlNode?): RelRoot?
    // CHECKSTYLE: IGNORE 1

    @Deprecated
    @Deprecated("Use {@link #rel}. ")
    @Throws(RelConversionException::class)
    fun  // to removed before 2.0
            convert(sql: SqlNode?): RelNode?

    /** Returns the type factory.  */
    val typeFactory: RelDataTypeFactory?

    /**
     * Converts one relational expression tree into another relational expression
     * based on a particular rule set and requires set of traits.
     *
     * @param ruleSetIndex The RuleSet to use for conversion purposes.  Note that
     * this is zero-indexed and is based on the list and order
     * of RuleSets provided in the construction of this
     * Planner.
     * @param requiredOutputTraits The set of RelTraits required of the root node
     * at the termination of the planning cycle.
     * @param rel The root of the RelNode tree to convert.
     * @return The root of the new RelNode tree.
     * @throws org.apache.calcite.tools.RelConversionException on conversion
     * error
     */
    @Throws(RelConversionException::class)
    fun transform(
        ruleSetIndex: Int,
        requiredOutputTraits: RelTraitSet?, rel: RelNode?
    ): RelNode?

    /**
     * Resets this `Planner` to be used with a new query. This
     * should be called between each new query.
     */
    fun reset()

    /**
     * Releases all internal resources utilized while this `Planner`
     * exists.  Once called, this Planner object is no longer valid.
     */
    @Override
    fun close()
    val emptyTraitSet: RelTraitSet?
}
