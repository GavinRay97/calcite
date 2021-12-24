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
package org.apache.calcite.jdbc

import org.apache.calcite.config.CalciteConnectionConfig

/**
 * A SqlValidator with schema and type factory of the given
 * [org.apache.calcite.jdbc.CalcitePrepare.Context].
 *
 *
 * This class is only used to derive data type for DDL sql node.
 * Usually we deduce query sql node data type(i.e. the `SqlSelect`)
 * during the validation phrase. DDL nodes don't have validation,
 * they can be executed directly through
 * [org.apache.calcite.server.DdlExecutor].
 *
 *
 * During the execution, [org.apache.calcite.sql.SqlDataTypeSpec] uses
 * this validator to derive its type.
 */
class ContextSqlValidator
/**
 * Create a `ContextSqlValidator`.
 * @param context Prepare context.
 * @param mutable Whether to get the mutable schema.
 */
    (context: CalcitePrepare.Context, mutable: Boolean) : SqlValidatorImpl(
    SqlStdOperatorTable.instance(), getCatalogReader(context, mutable),
    context.getTypeFactory(), Config.DEFAULT
) {
    companion object {
        private fun getCatalogReader(
            context: CalcitePrepare.Context, mutable: Boolean
        ): CalciteCatalogReader {
            return CalciteCatalogReader(
                if (mutable) context.getMutableRootSchema() else context.getRootSchema(),
                ImmutableList.of(),
                context.getTypeFactory(),
                CalciteConnectionConfig.DEFAULT
            )
        }
    }
}
