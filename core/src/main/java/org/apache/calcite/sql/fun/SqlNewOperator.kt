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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.sql.SqlCall

/**
 * SqlNewOperator represents an SQL `new specification` such as
 * `NEW UDT(1, 2)`. When used in an SqlCall, SqlNewOperator takes a
 * single operand, which is an invocation of the constructor method; but when
 * used in a RexCall, the operands are the initial values to be used for the new
 * instance.
 */
class SqlNewOperator  //~ Constructors -----------------------------------------------------------
    : SqlPrefixOperator("NEW", SqlKind.NEW_SPECIFICATION, 0, null, null, null) {
    //~ Methods ----------------------------------------------------------------
    // override SqlOperator
    @Override
    fun rewriteCall(validator: SqlValidator?, call: SqlCall): SqlNode {
        // New specification is purely syntactic, so we rewrite it as a
        // direct call to the constructor method.
        return call.operand(0)
    }

    // override SqlOperator
    @Override
    fun requiresDecimalExpansion(): Boolean {
        return false
    }
}
