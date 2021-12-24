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

import org.apache.calcite.rel.type.RelDataType

/**
 * SqlTypeTransforms defines a number of reusable instances of
 * [SqlTypeTransform].
 *
 *
 * NOTE: avoid anonymous inner classes here except for unique,
 * non-generalizable strategies; anything else belongs in a reusable top-level
 * class. If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 */
object SqlTypeTransforms {
    //~ Static fields/initializers ---------------------------------------------
    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but nullable if any of a calls operands is
     * nullable.
     */
    val TO_NULLABLE: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        SqlTypeUtil.makeNullableIfOperandsAre(
            opBinding.getTypeFactory(),
            opBinding.collectOperandTypes(),
            requireNonNull(typeToTransform, "typeToTransform")
        )
    }

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type, but nullable if and only if all of a call's
     * operands are nullable.
     */
    val TO_NULLABLE_ALL: SqlTypeTransform = SqlTypeTransform { opBinding, type ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        typeFactory.createTypeWithNullability(
            type,
            SqlTypeUtil.allNullable(opBinding.collectOperandTypes())
        )
    }

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but not nullable.
     */
    val TO_NOT_NULLABLE: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        opBinding.getTypeFactory().createTypeWithNullability(
            requireNonNull(typeToTransform, "typeToTransform"), false
        )
    }

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type with nulls allowed.
     */
    val FORCE_NULLABLE: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        opBinding.getTypeFactory().createTypeWithNullability(
            requireNonNull(typeToTransform, "typeToTransform"), true
        )
    }

    /**
     * Type-inference strategy whereby the result is NOT NULL if any of
     * the arguments is NOT NULL; otherwise the type is unchanged.
     */
    val LEAST_NULLABLE: SqlTypeTransform = label@ SqlTypeTransform { opBinding, typeToTransform ->
        for (type in opBinding.collectOperandTypes()) {
            if (!type.isNullable()) {
                return@label opBinding.getTypeFactory()
                    .createTypeWithNullability(typeToTransform, false)
            }
        }
        typeToTransform
    }

    /**
     * Type-inference strategy whereby the result type of a call is VARYING the
     * type given. The length returned is the same as length of the first
     * argument. Return type will have same nullability as input type
     * nullability. First Arg must be of string type.
     */
    val TO_VARYING: SqlTypeTransform = object : SqlTypeTransform() {
        @Override
        override fun transformType(
            opBinding: SqlOperatorBinding,
            typeToTransform: RelDataType
        ): RelDataType {
            when (typeToTransform.getSqlTypeName()) {
                VARCHAR, VARBINARY -> return typeToTransform
                else -> {}
            }
            val retTypeName: SqlTypeName = toVar(typeToTransform)
            var ret: RelDataType = opBinding.getTypeFactory().createSqlType(
                retTypeName,
                typeToTransform.getPrecision()
            )
            if (SqlTypeUtil.inCharFamily(typeToTransform)) {
                ret = opBinding.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        ret,
                        getCharset(typeToTransform),
                        getCollation(typeToTransform)
                    )
            }
            return opBinding.getTypeFactory().createTypeWithNullability(
                ret,
                typeToTransform.isNullable()
            )
        }

        private fun toVar(type: RelDataType): SqlTypeName {
            val sqlTypeName: SqlTypeName = org.apache.calcite.sql.type.type.getSqlTypeName()
            return when (org.apache.calcite.sql.type.sqlTypeName) {
                CHAR -> SqlTypeName.VARCHAR
                BINARY -> SqlTypeName.VARBINARY
                ANY -> SqlTypeName.ANY
                NULL -> SqlTypeName.NULL
                else -> throw Util.unexpected(org.apache.calcite.sql.type.sqlTypeName)
            }
        }
    }

    /**
     * Parameter type-inference transform strategy where a derived type must be
     * a multiset type and the returned type is the multiset's element type.
     *
     * @see MultisetSqlType.getComponentType
     */
    val TO_MULTISET_ELEMENT_TYPE: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        requireNonNull(
            typeToTransform.getComponentType()
        ) { "componentType for $typeToTransform in opBinding $opBinding" }
    }

    /**
     * Parameter type-inference transform strategy that wraps a given type
     * in a multiset.
     *
     * @see org.apache.calcite.rel.type.RelDataTypeFactory.createMultisetType
     */
    val TO_MULTISET: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        opBinding.getTypeFactory().createMultisetType(typeToTransform, -1)
    }

    /**
     * Parameter type-inference transform strategy that wraps a given type
     * in a array.
     *
     * @see org.apache.calcite.rel.type.RelDataTypeFactory.createArrayType
     */
    val TO_ARRAY: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        opBinding.getTypeFactory().createArrayType(typeToTransform, -1)
    }

    /**
     * Parameter type-inference transform strategy that converts a two-field
     * record type to a MAP type.
     *
     * @see org.apache.calcite.rel.type.RelDataTypeFactory.createMapType
     */
    val TO_MAP: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        SqlTypeUtil.createMapTypeFromRecord(
            opBinding.getTypeFactory(),
            typeToTransform
        )
    }

    /**
     * Parameter type-inference transform strategy where a derived type must be
     * a struct type with precisely one field and the returned type is the type
     * of that field.
     */
    val ONLY_COLUMN: SqlTypeTransform = SqlTypeTransform { opBinding, typeToTransform ->
        val fields: List<RelDataTypeField> = typeToTransform.getFieldList()
        assert(fields.size() === 1)
        fields[0].getType()
    }
}
