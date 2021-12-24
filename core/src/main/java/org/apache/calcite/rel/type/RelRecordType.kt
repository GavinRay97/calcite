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
package org.apache.calcite.rel.type

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.sql.type.SqlTypeName
import java.io.Serializable
import java.util.List
import java.util.Objects.requireNonNull

/**
 * RelRecordType represents a structured type having named fields.
 */
class RelRecordType(
    kind: StructKind?,
    fields: List<RelDataTypeField?>?,
    @get:Override override val isNullable: Boolean
) : RelDataTypeImpl(fields), Serializable {
    /** Name resolution policy; usually [StructKind.FULLY_QUALIFIED].  */
    private val kind: StructKind
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `RecordType`. This should only be called from a
     * factory method.
     * @param kind Name resolution policy
     * @param fields List of fields
     * @param nullable Whether this record type allows null values
     */
    init {
        this.kind = requireNonNull(kind, "kind")
        computeDigest()
    }

    /**
     * Creates a `RecordType`. This should only be called from a
     * factory method.
     * Shorthand for `RelRecordType(kind, fields, false)`.
     */
    constructor(kind: StructKind?, fields: List<RelDataTypeField?>?) : this(kind, fields, false) {}

    /**
     * Creates a `RecordType`. This should only be called from a
     * factory method.
     * Shorthand for `RelRecordType(StructKind.FULLY_QUALIFIED, fields, false)`.
     */
    constructor(fields: List<RelDataTypeField?>?) : this(StructKind.FULLY_QUALIFIED, fields, false) {}

    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val sqlTypeName: SqlTypeName
        get() = SqlTypeName.ROW

    // REVIEW: angel 18-Aug-2005 Put in fake implementation for precision
    @get:Override
    override val precision: Int
        get() =// REVIEW: angel 18-Aug-2005 Put in fake implementation for precision
            0

    @get:Override
    override val structKind: org.apache.calcite.rel.type.StructKind?
        get() = kind

    @Override
    protected override fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        sb.append("RecordType")
        when (kind) {
            PEEK_FIELDS -> sb.append(":peek")
            PEEK_FIELDS_DEFAULT -> sb.append(":peek_default")
            PEEK_FIELDS_NO_EXPAND -> sb.append(":peek_no_expand")
            else -> {}
        }
        sb.append("(")
        for (ord in Ord.zip(requireNonNull(fieldList, "fieldList"))) {
            if (ord.i > 0) {
                sb.append(", ")
            }
            val field: RelDataTypeField = ord.e
            if (withDetail) {
                sb.append(field.getType().getFullTypeString())
            } else {
                sb.append(field.getType().toString())
            }
            sb.append(" ")
            sb.append(field.getName())
        }
        sb.append(")")
    }

    /**
     * Per [Serializable] API, provides a replacement object to be written
     * during serialization.
     *
     *
     * This implementation converts this RelRecordType into a
     * SerializableRelRecordType, whose `readResolve` method converts
     * it back to a RelRecordType during deserialization.
     */
    private fun writeReplace(): Object {
        return SerializableRelRecordType(requireNonNull(fieldList, "fieldList"))
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Skinny object which has the same information content as a
     * [RelRecordType] but skips redundant stuff like digest and the
     * immutable list.
     */
    private class SerializableRelRecordType(fields: List<RelDataTypeField>) : Serializable {
        private val fields: List<RelDataTypeField>

        init {
            this.fields = fields
        }

        /**
         * Per [Serializable] API. See
         * [RelRecordType.writeReplace].
         */
        private fun readResolve(): Object {
            return RelRecordType(fields)
        }
    }
}
