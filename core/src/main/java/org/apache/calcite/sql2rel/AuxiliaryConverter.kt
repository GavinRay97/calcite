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
package org.apache.calcite.sql2rel

import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable

/** Converts an expression for a group window function (e.g. TUMBLE)
 * into an expression for an auxiliary group function (e.g. TUMBLE_START).
 *
 * @see SqlStdOperatorTable.TUMBLE_OLD
 */
interface AuxiliaryConverter {
    /** Converts an expression.
     *
     * @param rexBuilder Rex  builder
     * @param groupCall Call to the group function, e.g. "TUMBLE($2, 36000)"
     * @param e Expression holding result of the group function, e.g. "$0"
     *
     * @return Expression for auxiliary function, e.g. "$0 + 36000" converts
     * the result of TUMBLE to the result of TUMBLE_END
     */
    fun convert(rexBuilder: RexBuilder?, groupCall: RexNode?, e: RexNode?): RexNode?

    /** Simple implementation of [AuxiliaryConverter].  */
    class Impl(f: SqlFunction) : AuxiliaryConverter {
        private val f: SqlFunction

        init {
            this.f = f
        }

        @Override
        override fun convert(
            rexBuilder: RexBuilder, groupCall: RexNode,
            e: RexNode
        ): RexNode {
            return when (f.getKind()) {
                TUMBLE_START, HOP_START, SESSION_START, SESSION_END -> e
                TUMBLE_END -> rexBuilder.makeCall(
                    SqlStdOperatorTable.PLUS, e,
                    (groupCall as RexCall).operands.get(1)
                )
                HOP_END -> rexBuilder.makeCall(
                    SqlStdOperatorTable.PLUS, e,
                    (groupCall as RexCall).operands.get(2)
                )
                else -> throw AssertionError("unknown: $f")
            }
        }
    }
}
