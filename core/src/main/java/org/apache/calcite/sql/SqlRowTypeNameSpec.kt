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
 * A sql type name specification of row type.
 *
 *
 * The grammar definition in SQL-2011 IWD 9075-2:201?(E)
 * 6.1 &lt;data type&gt; is as following:
 * <blockquote><pre>
 * &lt;row type&gt; ::=
 * ROW &lt;row type body&gt;
 * &lt;row type body&gt; ::=
 * &lt;left paren&gt; &lt;field definition&gt;
 * [ { &lt;comma&gt; &lt;field definition&gt; }... ]
 * &lt;right paren&gt;
 *
 * &lt;field definition&gt; ::=
 * &lt;field name&gt; &lt;data type&gt;
</pre></blockquote> *
 *
 *
 * As a extended syntax to the standard SQL, each field type can have a
 * [ NULL | NOT NULL ] suffix specification, i.e.
 * Row(f0 int null, f1 varchar not null). The default is NOT NULL(not nullable).
 */
class SqlRowTypeNameSpec(
    pos: SqlParserPos?,
    fieldNames: List<SqlIdentifier>,
    fieldTypes: List<SqlDataTypeSpec>
) : SqlTypeNameSpec(SqlIdentifier(SqlTypeName.ROW.getName(), pos), pos) {
    private val fieldNames: List<SqlIdentifier>
    private val fieldTypes: List<SqlDataTypeSpec>

    /**
     * Creates a row type specification.
     *
     * @param pos        The parser position
     * @param fieldNames The field names
     * @param fieldTypes The field data types
     */
    init {
        Objects.requireNonNull(fieldNames, "fieldNames")
        Objects.requireNonNull(fieldTypes, "fieldTypes")
        assert(
            fieldNames.size() > 0 // there must be at least one field.
        )
        this.fieldNames = fieldNames
        this.fieldTypes = fieldTypes
    }

    fun getFieldNames(): List<SqlIdentifier> {
        return fieldNames
    }

    fun getFieldTypes(): List<SqlDataTypeSpec> {
        return fieldTypes
    }

    val arity: Int
        get() = fieldNames.size()

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.print(getTypeName().getSimple())
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")")
        for (p in Pair.zip(fieldNames, fieldTypes)) {
            writer.sep(",", false)
            p.left.unparse(writer, 0, 0)
            p.right.unparse(writer, leftPrec, rightPrec)
            val isNullable: Boolean = p.right.getNullable()
            if (isNullable != null && isNullable) {
                // Row fields default is not nullable.
                writer.print("NULL")
            }
        }
        writer.endList(frame)
    }

    @Override
    fun equalsDeep(node: SqlTypeNameSpec, litmus: Litmus): Boolean {
        if (node !is SqlRowTypeNameSpec) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node as SqlRowTypeNameSpec
        if (fieldNames.size() !== that.fieldNames.size()) {
            return litmus.fail("{} != {}", this, node)
        }
        for (i in 0 until fieldNames.size()) {
            if (!fieldNames[i].equalsDeep(that.fieldNames[i], litmus)) {
                return litmus.fail("{} != {}", this, node)
            }
        }
        if (fieldTypes.size() !== that.fieldTypes.size()) {
            return litmus.fail("{} != {}", this, node)
        }
        for (i in 0 until fieldTypes.size()) {
            if (!fieldTypes[i].equals(that.fieldTypes[i])) {
                return litmus.fail("{} != {}", this, node)
            }
        }
        return litmus.succeed()
    }

    @Override
    fun deriveType(sqlValidator: SqlValidator): RelDataType {
        val typeFactory: RelDataTypeFactory = sqlValidator.getTypeFactory()
        return typeFactory.createStructType(
            fieldTypes.stream()
                .map { dt -> dt.deriveType(sqlValidator) }
                .collect(Collectors.toList()),
            fieldNames.stream()
                .map(SqlIdentifier::toString)
                .collect(Collectors.toList()))
    }
}
