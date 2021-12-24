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
 * A `SqlTypeNameSpec` is a type name specification that allows user to
 * customize sql node unparsing and data type deriving.
 *
 *
 * To customize sql node unparsing, override the method
 * [.unparse].
 *
 *
 * To customize data type deriving, override the method
 * [.deriveType].
 */
abstract class SqlTypeNameSpec protected constructor(name: SqlIdentifier, pos: SqlParserPos) {
    private val typeName: SqlIdentifier
    private val pos: SqlParserPos

    /**
     * Creates a `SqlTypeNameSpec`.
     *
     * @param name Name of the type
     * @param pos  Parser position, must not be null
     */
    init {
        typeName = name
        this.pos = pos
    }

    /**
     * Derive type from this SqlTypeNameSpec.
     *
     * @param validator The sql validator
     * @return the `RelDataType` instance, throws exception if we could not
     * deduce the type
     */
    abstract fun deriveType(validator: SqlValidator?): RelDataType?

    /** Writes a SQL representation of this spec to a writer.  */
    abstract fun unparse(writer: SqlWriter?, leftPrec: Int, rightPrec: Int)

    /** Returns whether this spec is structurally equivalent to another spec.  */
    abstract fun equalsDeep(spec: SqlTypeNameSpec?, litmus: Litmus?): Boolean
    val parserPos: SqlParserPos
        get() = pos

    fun getTypeName(): SqlIdentifier {
        return typeName
    }
}
