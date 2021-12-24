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
import org.apache.calcite.sql.SqlIdentifier
import java.util.List

/**
 * Implementation of
 * [org.apache.calcite.sql.validate.SqlValidatorCatalogReader] that passes
 * all calls to a parent catalog reader.
 */
abstract class DelegatingSqlValidatorCatalogReader protected constructor(
    catalogReader: SqlValidatorCatalogReader
) : SqlValidatorCatalogReader {
    protected val catalogReader: SqlValidatorCatalogReader

    /**
     * Creates a DelegatingSqlValidatorCatalogReader.
     *
     * @param catalogReader Parent catalog reader
     */
    init {
        this.catalogReader = catalogReader
    }

    @Override
    @Nullable
    fun getTable(names: List<String?>?): SqlValidatorTable {
        return catalogReader.getTable(names)
    }

    @Override
    @Nullable
    fun getNamedType(typeName: SqlIdentifier?): RelDataType {
        return catalogReader.getNamedType(typeName)
    }

    @Override
    fun getAllSchemaObjectNames(names: List<String?>?): List<SqlMoniker> {
        return catalogReader.getAllSchemaObjectNames(names)
    }

    @get:Override
    val schemaPaths: List<List<String>>
        get() = catalogReader.getSchemaPaths()

    @Override
    fun <C : Object?> unwrap(aClass: Class<C>?): @Nullable C? {
        return catalogReader.unwrap(aClass)
    }
}
