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
package org.apache.calcite.sql.type

import org.apache.calcite.rel.type.RelDataTypeSystem

/**
 * BasicSqlType represents a standard atomic SQL type (excluding interval
 * types).
 *
 *
 * Instances of this class are immutable.
 */
class BasicSqlType private constructor(
    typeSystem: RelDataTypeSystem,
    typeName: SqlTypeName,
    nullable: Boolean,
    precision: Int,
    scale: Int,
    @Nullable collation: SqlCollation?,
    @Nullable wrappedCharset: SerializableCharset?
) : AbstractSqlType(typeName, nullable, null) {
    //~ Static fields/initializers ---------------------------------------------
    //~ Instance fields --------------------------------------------------------
    private val precision: Int
    private val scale: Int
    private val typeSystem: RelDataTypeSystem

    @Nullable
    private val collation: SqlCollation?

    @Nullable
    private val wrappedCharset: SerializableCharset?
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs a type with no parameters. This should only be called from a
     * factory method.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     */
    constructor(typeSystem: RelDataTypeSystem, typeName: SqlTypeName) : this(
        typeSystem, typeName, false, PRECISION_NOT_SPECIFIED,
        SCALE_NOT_SPECIFIED, null, null
    ) {
        checkPrecScale(typeName, false, false)
    }

    /**
     * Constructs a type with precision/length but no scale.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     * @param precision Precision (called length for some types)
     */
    constructor(
        typeSystem: RelDataTypeSystem, typeName: SqlTypeName,
        precision: Int
    ) : this(
        typeSystem, typeName, false, precision, SCALE_NOT_SPECIFIED, null,
        null
    ) {
        checkPrecScale(typeName, true, false)
    }

    /**
     * Constructs a type with precision/length and scale.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     * @param precision Precision (called length for some types)
     * @param scale Scale
     */
    constructor(
        typeSystem: RelDataTypeSystem, typeName: SqlTypeName,
        precision: Int, scale: Int
    ) : this(typeSystem, typeName, false, precision, scale, null, null) {
        checkPrecScale(typeName, true, true)
    }

    /** Internal constructor.  */
    init {
        this.typeSystem = Objects.requireNonNull(typeSystem, "typeSystem")
        this.precision = precision
        this.scale = scale
        this.collation = collation
        this.wrappedCharset = wrappedCharset
        computeDigest()
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Constructs a type with nullablity.
     */
    fun createWithNullability(nullable: Boolean): BasicSqlType {
        return if (nullable == this.isNullable) {
            this
        } else BasicSqlType(
            typeSystem, this.typeName, nullable,
            precision, scale, collation, wrappedCharset
        )
    }

    /**
     * Constructs a type with charset and collation.
     *
     *
     * This must be a character type.
     */
    fun createWithCharsetAndCollation(
        charset: Charset?,
        collation: SqlCollation?
    ): BasicSqlType {
        Preconditions.checkArgument(SqlTypeUtil.inCharFamily(this))
        return BasicSqlType(
            typeSystem, this.typeName, this.isNullable,
            precision, scale, collation,
            SerializableCharset.forCharset(charset)
        )
    }

    @Override
    fun getPrecision(): Int {
        return if (precision == PRECISION_NOT_SPECIFIED) {
            typeSystem.getDefaultPrecision(typeName)
        } else precision
    }

    @Override
    fun getScale(): Int {
        if (scale == SCALE_NOT_SPECIFIED) {
            when (typeName) {
                TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> return 0
                else -> {}
            }
        }
        return scale
    }

    @get:Nullable
    @get:Override
    val charset: Charset?
        get() = if (wrappedCharset == null) null else wrappedCharset.getCharset()

    @Override
    @Nullable
    fun getCollation(): SqlCollation? {
        return collation
    }

    // implement RelDataTypeImpl
    @Override
    protected fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        // Called to make the digest, which equals() compares;
        // so equivalent data types must produce identical type strings.
        sb.append(typeName.name())
        val printPrecision = precision != PRECISION_NOT_SPECIFIED
        val printScale = scale != SCALE_NOT_SPECIFIED
        if (printPrecision) {
            sb.append('(')
            sb.append(getPrecision())
            if (printScale) {
                sb.append(", ")
                sb.append(getScale())
            }
            sb.append(')')
        }
        if (!withDetail) {
            return
        }
        if (wrappedCharset != null
            && !SqlCollation.IMPLICIT.getCharset().equals(wrappedCharset.getCharset())
        ) {
            sb.append(" CHARACTER SET \"")
            sb.append(wrappedCharset.getCharset().name())
            sb.append("\"")
        }
        if (collation != null && collation !== SqlCollation.IMPLICIT && collation !== SqlCollation.COERCIBLE) {
            sb.append(" COLLATE \"")
            sb.append(collation.getCollationName())
            sb.append("\"")
        }
    }

    /**
     * Returns a value which is a limit for this type.
     *
     *
     * For example,
     *
     * <table border="1">
     * <caption>Limits</caption>
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th>
     * <th>limit</th>
     * <th>beyond</th>
     * <th>precision</th>
     * <th>scale</th>
     * <th>Returns</th>
    </tr> *
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
    </tr> *
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>true</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
    </tr> *
     * <tr>
     * <td>Integer</td>
     * <td>false</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
    </tr> *
     * <tr>
     * <td>Boolean</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>TRUE</td>
    </tr> *
     * <tr>
     * <td>Varchar</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>10</td>
     * <td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
    </tr> *
    </table> *
     *
     * @param sign   If true, returns upper limit, otherwise lower limit
     * @param limit  If true, returns value at or near to overflow; otherwise
     * value at or near to underflow
     * @param beyond If true, returns the value just beyond the limit, otherwise
     * the value at the limit
     * @return Limit value
     */
    @Nullable
    fun getLimit(
        sign: Boolean,
        limit: SqlTypeName.Limit,
        beyond: Boolean
    ): Object? {
        val precision = if (typeName.allowsPrec()) getPrecision() else -1
        val scale = if (typeName.allowsScale()) getScale() else -1
        return typeName.getLimit(
            sign,
            limit,
            beyond,
            precision,
            scale
        )
    }

    companion object {
        /** Throws if `typeName` does not allow the given combination of
         * precision and scale.  */
        protected fun checkPrecScale(
            typeName: SqlTypeName,
            precisionSpecified: Boolean, scaleSpecified: Boolean
        ) {
            if (!typeName.allowsPrecScale(precisionSpecified, scaleSpecified)) {
                throw AssertionError(
                    "typeName.allowsPrecScale("
                            + precisionSpecified + ", " + scaleSpecified + "): " + typeName
                )
            }
        }
    }
}
