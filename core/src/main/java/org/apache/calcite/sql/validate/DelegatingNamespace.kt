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
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.util.Pair
import java.util.List

/**
 * An implementation of [SqlValidatorNamespace] that delegates all methods
 * to an underlying object.
 */
abstract class DelegatingNamespace protected constructor(namespace: SqlValidatorNamespace) : SqlValidatorNamespace {
    //~ Instance fields --------------------------------------------------------
    protected val namespace: SqlValidatorNamespace
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a DelegatingNamespace.
     *
     * @param namespace Underlying namespace, to delegate to
     */
    init {
        this.namespace = namespace
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val validator: SqlValidator
        get() = namespace.getValidator()

    @get:Nullable
    @get:Override
    val table: SqlValidatorTable
        get() = namespace.getTable()

    @get:Override
    val rowType: RelDataType
        get() = namespace.getRowType()

    @get:Override
    val rowTypeSansSystemColumns: RelDataType
        get() = namespace.getRowTypeSansSystemColumns()

    @get:Override
    @set:Override
    var type: RelDataType
        get() = namespace.getType()
        set(type) {
            namespace.setType(type)
        }

    @Override
    fun validate(targetRowType: RelDataType?) {
        namespace.validate(targetRowType)
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = namespace.getNode()

    @get:Nullable
    @get:Override
    val enclosingNode: SqlNode
        get() = namespace.getEnclosingNode()

    @Override
    @Nullable
    fun lookupChild(
        name: String?
    ): SqlValidatorNamespace {
        return namespace.lookupChild(name)
    }

    @Override
    fun fieldExists(name: String?): Boolean {
        return namespace.fieldExists(name)
    }

    @get:Override
    val monotonicExprs: List<Any>
        get() = namespace.getMonotonicExprs()

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        return namespace.getMonotonicity(columnName)
    }

    @SuppressWarnings("deprecation")
    @Override
    fun makeNullable() {
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): T {
        return if (clazz.isInstance(this)) {
            clazz.cast(this)
        } else {
            namespace.unwrap(clazz)
        }
    }

    @Override
    fun isWrapperFor(clazz: Class<*>): Boolean {
        return (clazz.isInstance(this)
                || namespace.isWrapperFor(clazz))
    }
}
