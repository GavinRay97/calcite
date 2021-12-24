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
import org.apache.calcite.runtime.CalciteException
import org.apache.calcite.runtime.Resources
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlValidatorException
import java.util.List

/**
 * `ExplicitOperatorBinding` implements [SqlOperatorBinding]
 * via an underlying array of known operand types.
 */
class ExplicitOperatorBinding private constructor(
    @Nullable delegate: SqlOperatorBinding?,
    typeFactory: RelDataTypeFactory,
    operator: SqlOperator,
    types: List<RelDataType>
) : SqlOperatorBinding(typeFactory, operator) {
    //~ Instance fields --------------------------------------------------------
    private val types: List<RelDataType>

    @Nullable
    private val delegate: SqlOperatorBinding?

    //~ Constructors -----------------------------------------------------------
    constructor(
        delegate: SqlOperatorBinding,
        types: List<RelDataType>
    ) : this(
        delegate,
        delegate.getTypeFactory(),
        delegate.getOperator(),
        types
    ) {
    }

    constructor(
        typeFactory: RelDataTypeFactory,
        operator: SqlOperator,
        types: List<RelDataType>
    ) : this(null, typeFactory, operator, types) {
    }

    init {
        this.types = types
        this.delegate = delegate
    }

    //~ Methods ----------------------------------------------------------------
    // implement SqlOperatorBinding
    @get:Override
    val operandCount: Int
        get() = types.size()

    // implement SqlOperatorBinding
    @Override
    fun getOperandType(ordinal: Int): RelDataType {
        return types[ordinal]
    }

    @Override
    fun newError(
        e: Resources.ExInst<SqlValidatorException?>?
    ): CalciteException {
        return if (delegate != null) {
            delegate.newError(e)
        } else {
            SqlUtil.newContextException(SqlParserPos.ZERO, e)
        }
    }

    @Override
    fun isOperandNull(ordinal: Int, allowCast: Boolean): Boolean {
        // NOTE jvs 1-May-2006:  This call is only relevant
        // for SQL validation, so anywhere else, just say
        // everything's OK.
        return false
    }
}
