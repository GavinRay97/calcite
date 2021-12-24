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
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlVisitor
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Litmus
import java.util.Objects
import java.util.TimeZone

/**
 * Represents a SQL data type specification in a parse tree.
 *
 *
 * A `SqlDataTypeSpec` is immutable; once created, you cannot
 * change any of the fields.
 *
 *
 * We support the following data type expressions:
 *
 *
 *  * Complex data type expression like:
 * <blockquote>`ROW(<br></br>
 * foo NUMBER(5, 2) NOT NULL,<br></br>
 * rec ROW(b BOOLEAN, i MyUDT NOT NULL))`</blockquote>
 * Internally we use [SqlRowTypeNameSpec] to specify row data type name.
 *
 *  * Simple data type expression like CHAR, VARCHAR and DOUBLE
 * with optional precision and scale;
 * Internally we use [SqlBasicTypeNameSpec] to specify basic sql data type name.
 *
 *  * Collection data type expression like:
 * <blockquote>`
 * INT ARRAY;
 * VARCHAR(20) MULTISET;
 * INT ARRAY MULTISET;`</blockquote>
 * Internally we use [SqlCollectionTypeNameSpec] to specify collection data type name.
 *
 *  * User defined data type expression like `My_UDT`;
 * Internally we use [SqlUserDefinedTypeNameSpec] to specify user defined data type name.
 *
 *
 */
class SqlDataTypeSpec(
    typeNameSpec: SqlTypeNameSpec,
    @Nullable timeZone: TimeZone?,
    @Nullable nullable: Boolean,
    pos: SqlParserPos?
) : SqlNode(pos) {
    //~ Instance fields --------------------------------------------------------
    private val typeNameSpec: SqlTypeNameSpec

    @Nullable
    private val timeZone: TimeZone?

    /** Whether data type allows nulls.
     *
     *
     * Nullable is nullable! Null means "not specified". E.g.
     * `CAST(x AS INTEGER)` preserves the same nullability as `x`.
     */
    @Nullable
    val nullable: Boolean
        @Nullable get
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a type specification representing a type.
     *
     * @param typeNameSpec The type name can be basic sql type, row type,
     * collections type and user defined type
     */
    constructor(
        typeNameSpec: SqlTypeNameSpec,
        pos: SqlParserPos?
    ) : this(typeNameSpec, null, null, pos) {
    }

    /**
     * Creates a type specification representing a type, with time zone specified.
     *
     * @param typeNameSpec The type name can be basic sql type, row type,
     * collections type and user defined type
     * @param timeZone     Specified time zone
     */
    constructor(
        typeNameSpec: SqlTypeNameSpec,
        @Nullable timeZone: TimeZone?,
        pos: SqlParserPos?
    ) : this(typeNameSpec, timeZone, null, pos) {
    }

    /**
     * Creates a type specification representing a type, with time zone,
     * nullability and base type name specified.
     *
     * @param typeNameSpec The type name can be basic sql type, row type,
     * collections type and user defined type
     * @param timeZone     Specified time zone
     * @param nullable     The nullability
     */
    init {
        this.typeNameSpec = typeNameSpec
        this.timeZone = timeZone
        this.nullable = nullable
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun clone(pos: SqlParserPos?): SqlNode {
        return SqlDataTypeSpec(typeNameSpec, timeZone, pos)
    }

    @Override
    fun getMonotonicity(@Nullable scope: SqlValidatorScope?): SqlMonotonicity {
        return SqlMonotonicity.CONSTANT
    }

    val collectionsTypeName: SqlIdentifier?
        @Nullable get() = if (typeNameSpec is SqlCollectionTypeNameSpec) {
            typeNameSpec.getTypeName()
        } else null
    val typeName: SqlIdentifier
        get() = typeNameSpec.getTypeName()

    fun getTypeNameSpec(): SqlTypeNameSpec {
        return typeNameSpec
    }

    @Nullable
    fun getTimeZone(): TimeZone? {
        return timeZone
    }

    /** Returns a copy of this data type specification with a given
     * nullability.  */
    fun withNullable(nullable: Boolean): SqlDataTypeSpec {
        return withNullable(nullable, SqlParserPos.ZERO)
    }

    /** Returns a copy of this data type specification with a given
     * nullability, extending the parser position.  */
    fun withNullable(nullable: Boolean, pos: SqlParserPos): SqlDataTypeSpec {
        val newPos: SqlParserPos = if (pos === SqlParserPos.ZERO) pos else pos.plus(pos)
        return if (Objects.equals(nullable, this.nullable)
            && newPos.equals(pos)
        ) {
            this
        } else SqlDataTypeSpec(typeNameSpec, timeZone, nullable, newPos)
    }

    /**
     * Returns a new SqlDataTypeSpec corresponding to the component type if the
     * type spec is a collections type spec.<br></br>
     * Collection types are `ARRAY` and `MULTISET`.
     */
    val componentTypeSpec: SqlDataTypeSpec
        get() {
            assert(typeNameSpec is SqlCollectionTypeNameSpec)
            val elementTypeName: SqlTypeNameSpec = (typeNameSpec as SqlCollectionTypeNameSpec).getElementTypeName()
            return SqlDataTypeSpec(elementTypeName, timeZone, getParserPosition())
        }

    @Override
    fun unparse(writer: SqlWriter?, leftPrec: Int, rightPrec: Int) {
        typeNameSpec.unparse(writer, leftPrec, rightPrec)
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateDataType(this)
    }

    @Override
    fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Override
    fun equalsDeep(@Nullable node: SqlNode, litmus: Litmus): Boolean {
        if (node !is SqlDataTypeSpec) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node as SqlDataTypeSpec
        if (!Objects.equals(timeZone, that.timeZone)) {
            return litmus.fail("{} != {}", this, node)
        }
        return if (!typeNameSpec.equalsDeep(that.typeNameSpec, litmus)) {
            litmus.fail(null)
        } else litmus.succeed()
    }

    /**
     * Converts this type specification to a [RelDataType].
     *
     *
     * Throws an error if the type is not found.
     */
    fun deriveType(validator: SqlValidator): RelDataType {
        return deriveType(validator, false)
    }

    /**
     * Converts this type specification to a [RelDataType].
     *
     *
     * Throws an error if the type is not found.
     *
     * @param nullable Whether the type is nullable if the type specification
     * does not explicitly state
     */
    fun deriveType(validator: SqlValidator, nullable: Boolean): RelDataType {
        var type: RelDataType
        type = typeNameSpec.deriveType(validator)

        // Fix-up the nullability, default is false.
        val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
        type = fixUpNullability(typeFactory, type, nullable)
        return type
    }
    //~ Tools ------------------------------------------------------------------
    /**
     * Fix up the nullability of the `type`.
     *
     * @param typeFactory Type factory
     * @param type        The type to coerce nullability
     * @param nullable    Default nullability to use if this type specification does not
     * specify nullability
     * @return Type with specified nullability or the default(false)
     */
    private fun fixUpNullability(
        typeFactory: RelDataTypeFactory,
        type: RelDataType, nullable: Boolean
    ): RelDataType {
        var nullable = nullable
        if (this.nullable != null) {
            nullable = this.nullable
        }
        return typeFactory.createTypeWithNullability(type, nullable)
    }
}
