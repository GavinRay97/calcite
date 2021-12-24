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

import org.apache.calcite.rel.type.RelDataType

/**
 * A sql type name specification of user defined type.
 *
 *
 * Usually you should register the UDT into the [org.apache.calcite.jdbc.CalciteSchema]
 * first before referencing it in the sql statement.
 */
class SqlUserDefinedTypeNameSpec
/**
 * Create a SqlUserDefinedTypeNameSpec instance.
 *
 * @param typeName Type name as SQL identifier
 * @param pos The parser position
 */
    (typeName: SqlIdentifier, pos: SqlParserPos) : SqlTypeNameSpec(typeName, pos) {
    constructor(name: String?, pos: SqlParserPos?) : this(SqlIdentifier(name, pos), pos) {}

    @Override
    override fun deriveType(validator: SqlValidator): RelDataType {
        // The type name is a compound identifier, that means it is a UDT,
        // use SqlValidator to deduce its type from the Schema.
        return validator.getValidatedNodeType(getTypeName())
    }

    @Override
    override fun unparse(writer: SqlWriter?, leftPrec: Int, rightPrec: Int) {
        getTypeName().unparse(writer, leftPrec, rightPrec)
    }

    @Override
    override fun equalsDeep(spec: SqlTypeNameSpec?, litmus: Litmus): Boolean {
        if (spec !is SqlUserDefinedTypeNameSpec) {
            return litmus.fail("{} != {}", this, spec)
        }
        return if (!this.getTypeName().equalsDeep(spec.getTypeName(), litmus)) {
            litmus.fail("{} != {}", this, spec)
        } else litmus.succeed()
    }
}
