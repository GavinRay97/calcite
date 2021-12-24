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
package org.apache.calcite.rex

import org.apache.calcite.sql.SqlOperator
import java.util.HashMap
import java.util.Map

/**
 * Implementation of [RexSqlConvertletTable].
 */
class RexSqlReflectiveConvertletTable  //~ Constructors -----------------------------------------------------------
    : RexSqlConvertletTable {
    //~ Instance fields --------------------------------------------------------
    private val map: Map<Object, Object> = HashMap()

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    override operator fun get(call: RexCall): RexSqlConvertlet? {
        var convertlet: RexSqlConvertlet?
        val op: SqlOperator = call.getOperator()

        // Is there a convertlet for this operator
        // (e.g. SqlStdOperatorTable.plusOperator)?
        convertlet = map[op]
        if (convertlet != null) {
            return convertlet
        }

        // Is there a convertlet for this class of operator
        // (e.g. SqlBinaryOperator)?
        @Nullable var clazz: Class<*> = op.getClass()
        while (clazz != null) {
            convertlet = map[clazz]
            if (convertlet != null) {
                return convertlet
            }
            clazz = clazz.getSuperclass()
        }

        // Is there a convertlet for this class of expression
        // (e.g. SqlCall)?
        clazz = call.getClass()
        while (clazz != null) {
            convertlet = map[clazz]
            if (convertlet != null) {
                return convertlet
            }
            clazz = clazz.getSuperclass()
        }
        return null
    }

    /**
     * Registers a convertlet for a given operator instance.
     *
     * @param op         Operator instance, say
     * [org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS]
     * @param convertlet Convertlet
     */
    protected fun registerOp(op: SqlOperator?, convertlet: RexSqlConvertlet?) {
        map.put(op, convertlet)
    }
}
