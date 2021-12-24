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
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import java.util.List
import java.util.Objects
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Abstract implementation of [SqlValidatorNamespace].
 */
internal abstract class AbstractNamespace(
    validator: SqlValidatorImpl,
    @Nullable enclosingNode: SqlNode
) : SqlValidatorNamespace {
    //~ Instance fields --------------------------------------------------------
    protected val validator: SqlValidatorImpl

    /**
     * Whether this scope is currently being validated. Used to check for
     * cycles.
     */
    private var status: SqlValidatorImpl.Status = SqlValidatorImpl.Status.UNVALIDATED

    /**
     * Type of the output row, which comprises the name and type of each output
     * column. Set on validate.
     */
    @Nullable
    protected var rowType: RelDataType? = null

    /** As [.rowType], but not necessarily a struct.  */
    @Nullable
    protected var type: RelDataType? = null

    @Nullable
    protected val enclosingNode: SqlNode
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AbstractNamespace.
     *
     * @param validator     Validator
     * @param enclosingNode Enclosing node
     */
    init {
        this.validator = validator
        this.enclosingNode = enclosingNode
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getValidator(): SqlValidator {
        return validator
    }

    @Override
    fun validate(targetRowType: RelDataType?) {
        when (status) {
            UNVALIDATED -> try {
                status = SqlValidatorImpl.Status.IN_PROGRESS
                Preconditions.checkArgument(
                    rowType == null,
                    "Namespace.rowType must be null before validate has been called"
                )
                val type: RelDataType? = validateImpl(targetRowType)
                Preconditions.checkArgument(
                    type != null,
                    "validateImpl() returned null"
                )
                setType(type)
            } finally {
                status = SqlValidatorImpl.Status.VALID
            }
            IN_PROGRESS -> throw AssertionError("Cycle detected during type-checking")
            VALID -> {}
            else -> throw Util.unexpected(status)
        }
    }

    /**
     * Validates this scope and returns the type of the records it returns.
     * External users should call [.validate], which uses the
     * [.status] field to protect against cycles.
     *
     * @return record data type, never null
     *
     * @param targetRowType Desired row type, must not be null, may be the data
     * type 'unknown'.
     */
    protected abstract fun validateImpl(targetRowType: RelDataType?): RelDataType?
    @Override
    fun getRowType(): RelDataType? {
        if (rowType == null) {
            validator.validateNamespace(this, validator.unknownType)
            Objects.requireNonNull(rowType, "validate must set rowType")
        }
        return rowType
    }

    @get:Override
    val rowTypeSansSystemColumns: RelDataType?
        get() = getRowType()

    @Override
    fun getType(): RelDataType {
        Util.discard(getRowType())
        return Objects.requireNonNull(type, "type")
    }

    @Override
    fun setType(type: RelDataType?) {
        this.type = type
        rowType = convertToStruct(type)
    }

    @Override
    @Nullable
    fun getEnclosingNode(): SqlNode {
        return enclosingNode
    }

    @get:Nullable
    @get:Override
    val table: SqlValidatorTable?
        get() = null

    @Override
    @Nullable
    fun lookupChild(name: String?): SqlValidatorNamespace {
        return validator.lookupFieldNamespace(
            getRowType(),
            name
        )
    }

    @Override
    fun fieldExists(name: String?): Boolean {
        val rowType: RelDataType? = getRowType()
        return validator.catalogReader.nameMatcher().field(rowType, name) != null
    }

    @get:Override
    val monotonicExprs: List<Any>
        get() = ImmutableList.of()

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        return SqlMonotonicity.NOT_MONOTONIC
    }

    @SuppressWarnings("deprecation")
    @Override
    fun makeNullable() {
    }

    fun translate(name: String): String {
        return name
    }

    @Override
    fun resolve(): SqlValidatorNamespace {
        return this
    }

    @Override
    fun supportsModality(modality: SqlModality?): Boolean {
        return true
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): T {
        return clazz.cast(this)
    }

    @Override
    fun isWrapperFor(clazz: Class<*>): Boolean {
        return clazz.isInstance(this)
    }

    protected fun convertToStruct(type: RelDataType?): RelDataType? {
        // "MULTISET [<expr>, ...]" needs to be wrapped in a record if
        // <expr> has a scalar type.
        // For example, "MULTISET [8, 9]" has type
        // "RECORD(INTEGER EXPR$0 NOT NULL) NOT NULL MULTISET NOT NULL".
        val componentType: RelDataType = type.getComponentType()
        if (componentType == null || componentType.isStruct()) {
            return type
        }
        val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
        val structType: RelDataType = toStruct(componentType, getNode())
        val collectionType: RelDataType
        collectionType = when (type.getSqlTypeName()) {
            ARRAY -> typeFactory.createArrayType(structType, -1)
            MULTISET -> typeFactory.createMultisetType(structType, -1)
            else -> throw AssertionError(type)
        }
        return typeFactory.createTypeWithNullability(
            collectionType,
            type.isNullable()
        )
    }

    /** Converts a type to a struct if it is not already.  */
    protected fun toStruct(type: RelDataType, @Nullable unnest: SqlNode?): RelDataType {
        return if (type.isStruct()) {
            type
        } else validator.getTypeFactory().builder()
            .add(
                castNonNull(
                    validator.deriveAlias(
                        Objects.requireNonNull(unnest, "unnest"),
                        0
                    )
                ),
                type
            )
            .build()
    }
}
