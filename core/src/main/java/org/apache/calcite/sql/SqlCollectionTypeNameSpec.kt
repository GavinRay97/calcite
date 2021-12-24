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
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Util
import java.util.Objects

/**
 * A sql type name specification of collection type.
 *
 *
 * The grammar definition in SQL-2011 IWD 9075-2:201?(E)
 * 6.1 &lt;collection type&gt; is as following:
 * <blockquote><pre>
 * &lt;collection type&gt; ::=
 * &lt;array type&gt;
 * | &lt;multiset type&gt;
 *
 * &lt;array type&gt; ::=
 * &lt;data type&gt; ARRAY
 * [ &lt;left bracket or trigraph&gt;
 * &lt;maximum cardinality&gt;
 * &lt;right bracket or trigraph&gt; ]
 *
 * &lt;maximum cardinality&gt; ::=
 * &lt;unsigned integer&gt;
 *
 * &lt;multiset type&gt; ::=
 * &lt;data type&gt; MULTISET
</pre></blockquote> *
 *
 *
 * This class is intended to describe SQL collection type. It can describe
 * either simple collection type like "int array" or nested collection type like
 * "int array array" or "int array multiset". For nested collection type, the element type
 * name of this `SqlCollectionTypeNameSpec` is also a `SqlCollectionTypeNameSpec`.
 */
class SqlCollectionTypeNameSpec(
    elementTypeName: SqlTypeNameSpec?,
    collectionTypeName: SqlTypeName,
    pos: SqlParserPos?
) : SqlTypeNameSpec(SqlIdentifier(collectionTypeName.name(), pos), pos) {
    private val elementTypeName: SqlTypeNameSpec
    private val collectionTypeName: SqlTypeName

    /**
     * Creates a `SqlCollectionTypeNameSpec`.
     *
     * @param elementTypeName    Type of the collection element
     * @param collectionTypeName Collection type name
     * @param pos                Parser position, must not be null
     */
    init {
        this.elementTypeName = Objects.requireNonNull(elementTypeName, "elementTypeName")
        this.collectionTypeName = Objects.requireNonNull(collectionTypeName, "collectionTypeName")
    }

    fun getElementTypeName(): SqlTypeNameSpec {
        return elementTypeName
    }

    @Override
    fun deriveType(validator: SqlValidator): RelDataType {
        val type: RelDataType = elementTypeName.deriveType(validator)
        return createCollectionType(type, validator.getTypeFactory())
    }

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        elementTypeName.unparse(writer, leftPrec, rightPrec)
        writer.keyword(collectionTypeName.name())
    }

    @Override
    fun equalsDeep(spec: SqlTypeNameSpec, litmus: Litmus): Boolean {
        if (spec !is SqlCollectionTypeNameSpec) {
            return litmus.fail("{} != {}", this, spec)
        }
        val that = spec as SqlCollectionTypeNameSpec
        if (!elementTypeName.equalsDeep(that.elementTypeName, litmus)) {
            return litmus.fail("{} != {}", this, spec)
        }
        return if (!Objects.equals(collectionTypeName, that.collectionTypeName)) {
            litmus.fail("{} != {}", this, spec)
        } else litmus.succeed()
    }
    //~ Tools ------------------------------------------------------------------
    /**
     * Create collection data type.
     *
     * @param elementType Type of the collection element
     * @param typeFactory Type factory
     * @return The collection data type, or throw exception if the collection
     * type name does not belong to `SqlTypeName` enumerations
     */
    private fun createCollectionType(
        elementType: RelDataType,
        typeFactory: RelDataTypeFactory
    ): RelDataType {
        return when (collectionTypeName) {
            MULTISET -> typeFactory.createMultisetType(elementType, -1)
            ARRAY -> typeFactory.createArrayType(elementType, -1)
            else -> throw Util.unexpected(collectionTypeName)
        }
    }
}
