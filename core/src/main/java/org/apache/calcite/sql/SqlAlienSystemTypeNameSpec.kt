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

import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.Litmus
import java.util.Objects

/**
 * Represents a type name for an alien system. For example,
 * UNSIGNED is a built-in type in MySQL which is synonym of INTEGER.
 *
 *
 * You can use this class to define a customized type name with specific alias,
 * for example, in some systems, STRING is synonym of VARCHAR
 * and BYTES is synonym of VARBINARY.
 *
 *
 * Internally we may use the [SqlAlienSystemTypeNameSpec] to unparse
 * as the builtin data type name for some alien systems during rel-to-sql conversion.
 */
class SqlAlienSystemTypeNameSpec
/**
 * Creates a `SqlAlienSystemTypeNameSpec` instance.
 *
 * @param typeAlias Type alias of the alien system
 * @param typeName  Type name the `typeAlias` implies as the (standard) basic type name
 * @param precision Type Precision
 * @param pos       The parser position
 */(
    //~ Instance fields --------------------------------------------------------
    // Type alias used for unparsing.
    private val typeAlias: String?,
    typeName: SqlTypeName,
    precision: Int,
    pos: SqlParserPos?
) : SqlBasicTypeNameSpec(typeName, precision, pos) {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `SqlAlienSystemTypeNameSpec` instance.
     *
     * @param typeAlias Type alias of the alien system
     * @param typeName  Type name the `typeAlias` implies as the (standard) basic type name
     * @param pos       The parser position
     */
    constructor(
        typeAlias: String?,
        typeName: SqlTypeName,
        pos: SqlParserPos?
    ) : this(typeAlias, typeName, -1, pos) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword(typeAlias)
    }

    @Override
    override fun equalsDeep(node: SqlTypeNameSpec, litmus: Litmus): Boolean {
        if (node !is SqlAlienSystemTypeNameSpec) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node as SqlAlienSystemTypeNameSpec
        return if (!Objects.equals(typeAlias, that.typeAlias)) {
            litmus.fail("{} != {}", this, node)
        } else super.equalsDeep(node, litmus)
    }
}
