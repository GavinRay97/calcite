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
package org.apache.calcite.sql.validate

import org.apache.calcite.rel.type.RelDataType

/** Namespace based on a schema.
 *
 *
 * The visible names are tables and sub-schemas.
 */
internal class SchemaNamespace(validator: SqlValidatorImpl?, names: ImmutableList<String?>?) :
    AbstractNamespace(validator, null) {
    /** The path of this schema.  */
    private val names: ImmutableList<String>

    /** Creates a SchemaNamespace.  */
    init {
        this.names = Objects.requireNonNull(names, "names")
    }

    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        val builder: RelDataTypeFactory.Builder = validator.getTypeFactory().builder()
        for (moniker in validator.catalogReader.getAllSchemaObjectNames(names)) {
            val names1: List<String> = moniker.getFullyQualifiedNames()
            val table: SqlValidatorTable = requireNonNull(
                validator.catalogReader.getTable(names1)
            ) { "table $names1 is not found in scope $names" }
            builder.add(Util.last(names1), table.getRowType())
        }
        return builder.build()
    }

    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = null
}
