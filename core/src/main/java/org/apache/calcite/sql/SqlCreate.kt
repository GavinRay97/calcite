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
package org.apache.calcite.sql

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * Base class for an CREATE statements parse tree nodes. The portion of the
 * statement covered by this class is "CREATE [ OR REPLACE ]". Subclasses handle
 * whatever comes afterwards.
 */
abstract class SqlCreate
/** Creates a SqlCreate.  */ protected constructor(
    operator: SqlOperator?, pos: SqlParserPos?,
    /** Whether "OR REPLACE" was specified.  */
    var replace: Boolean,
    /** Whether "IF NOT EXISTS" was specified.  */
    val ifNotExists: Boolean
) : SqlDdl(operator, pos) {

    @Deprecated // to be removed before 2.0
    protected constructor(pos: SqlParserPos?, replace: Boolean) : this(SqlDdl.DDL_OPERATOR, pos, replace, false) {
    }
}
