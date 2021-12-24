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
package org.apache.calcite.rex

import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode

/**
 * Converts expressions from [RexNode] to [SqlNode].
 *
 *
 * For most purposes, [org.apache.calcite.rel.rel2sql.SqlImplementor]
 * is superior. See in particular
 * [org.apache.calcite.rel.rel2sql.SqlImplementor.Context.toSql].
 */
interface RexToSqlNodeConverter {
    //~ Methods ----------------------------------------------------------------
    /**
     * Converts a [RexNode] to a [SqlNode] expression,
     * typically by dispatching to one of the other interface methods.
     *
     * @param node RexNode to translate
     * @return SqlNode, or null if no translation was available
     */
    @Nullable
    fun convertNode(node: RexNode?): SqlNode?

    /**
     * Converts a [RexCall] to a [SqlNode] expression.
     *
     * @param call RexCall to translate
     * @return SqlNode, or null if no translation was available
     */
    @Nullable
    fun convertCall(call: RexCall?): SqlNode?

    /**
     * Converts a [RexLiteral] to a [SqlLiteral].
     *
     * @param literal RexLiteral to translate
     * @return SqlNode, or null if no translation was available
     */
    @Nullable
    fun convertLiteral(literal: RexLiteral?): SqlNode?

    /**
     * Converts a [RexInputRef] to a [SqlIdentifier].
     *
     * @param ref RexInputRef to translate
     * @return SqlNode, or null if no translation was available
     */
    @Nullable
    fun convertInputRef(ref: RexInputRef?): SqlNode?
}
