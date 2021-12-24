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
package org.apache.calcite.schema

import org.apache.calcite.linq4j.Queryable

/**
 * A named expression in a schema.
 *
 * <h2>Examples of members</h2>
 *
 *
 * Several kinds of members crop up in real life. They all implement the
 * `Member` interface, but tend to be treated differently by the
 * back-end system if not by Calcite.
 *
 *
 * A member that has zero arguments and a type that is a collection of
 * records is referred to as a *relation*. In schemas backed by a
 * relational database, tables and views will appear as relations.
 *
 *
 * A member that has one or more arguments and a type that is a collection
 * of records is referred to as a *parameterized relation*. Some relational
 * databases support these; for example, Oracle calls them "table
 * functions".
 *
 *
 * Members may be also more typical of programming-language functions:
 * they take zero or more arguments, and return a result of arbitrary type.
 *
 *
 * From the above definitions, you can see that a member is a special
 * kind of function. This makes sense, because even though it has no
 * arguments, it is "evaluated" each time it is used in a query.
 */
interface Member {
    /**
     * The name of this function.
     */
    val name: String?

    /**
     * Returns the parameters of this member.
     *
     * @return Parameters; never null
     */
    val parameters: List<org.apache.calcite.schema.FunctionParameter?>?

    /**
     * Returns the type of this function's result.
     *
     * @return Type of result; never null
     */
    val type: RelDataType?

    /**
     * Evaluates this member to yield a result. The result is a
     * [org.apache.calcite.linq4j.Queryable].
     *
     * @param schemaInstance Object that is an instance of the containing
     * [Schema]
     * @param arguments List of arguments to the call; must match
     * [parameters][.getParameters] in number and type
     *
     * @return An instance of this schema object, as a Queryable
     */
    fun evaluate(
        schemaInstance: Object?,
        arguments: List<Object?>?
    ): Queryable?
}
