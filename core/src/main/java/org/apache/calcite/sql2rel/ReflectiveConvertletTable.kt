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
package org.apache.calcite.sql2rel

import org.apache.calcite.rex.RexNode

/**
 * Implementation of [SqlRexConvertletTable] which uses reflection to call
 * any method of the form `public RexNode convertXxx(ConvertletContext,
 * SqlNode)` or `public RexNode convertXxx(ConvertletContext,
 * SqlOperator, SqlCall)`.
 */
class ReflectiveConvertletTable : SqlRexConvertletTable {
    //~ Instance fields --------------------------------------------------------
    private val map: Map<Object?, Object> = HashMap()

    //~ Constructors -----------------------------------------------------------
    init {
        for (method in getClass().getMethods()) {
            registerNodeTypeMethod(method)
            registerOpTypeMethod(method)
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Registers method if it: a. is public, and b. is named "convertXxx", and
     * c. has a return type of "RexNode" or a subtype d. has a 2 parameters with
     * types ConvertletContext and SqlNode (or a subtype) respectively.
     */
    @RequiresNonNull("map")
    private fun registerNodeTypeMethod(
        method: Method
    ) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return
        }
        if (!method.getName().startsWith("convert")) {
            return
        }
        if (!RexNode::class.java.isAssignableFrom(method.getReturnType())) {
            return
        }
        val parameterTypes: Array<Class> = method.getParameterTypes()
        if (parameterTypes.size != 2) {
            return
        }
        if (parameterTypes[0] !== SqlRexContext::class.java) {
            return
        }
        val parameterType: Class = parameterTypes[1]
        if (!SqlNode::class.java.isAssignableFrom(parameterType)) {
            return
        }
        map.put(parameterType, label@ SqlRexConvertlet { cx, call ->
            try {
                @SuppressWarnings("argument.type.incompatible") val result: RexNode = method.invoke(
                    this@ReflectiveConvertletTable,
                    cx, call
                ) as RexNode
                return@label requireNonNull(result) {
                    ("null result from " + method
                            + " for call " + call)
                }
            } catch (e: IllegalAccessException) {
                throw RuntimeException("while converting $call", e)
            } catch (e: InvocationTargetException) {
                throw RuntimeException("while converting $call", e)
            }
        } as SqlRexConvertlet?)
    }

    /**
     * Registers method if it: a. is public, and b. is named "convertXxx", and
     * c. has a return type of "RexNode" or a subtype d. has a 3 parameters with
     * types: ConvertletContext; SqlOperator (or a subtype), SqlCall (or a
     * subtype).
     */
    @RequiresNonNull("map")
    private fun registerOpTypeMethod(
        method: Method
    ) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return
        }
        if (!method.getName().startsWith("convert")) {
            return
        }
        if (!RexNode::class.java.isAssignableFrom(method.getReturnType())) {
            return
        }
        val parameterTypes: Array<Class> = method.getParameterTypes()
        if (parameterTypes.size != 3) {
            return
        }
        if (parameterTypes[0] !== SqlRexContext::class.java) {
            return
        }
        val opClass: Class = parameterTypes[1]
        if (!SqlOperator::class.java.isAssignableFrom(opClass)) {
            return
        }
        val parameterType: Class = parameterTypes[2]
        if (!SqlCall::class.java.isAssignableFrom(parameterType)) {
            return
        }
        map.put(opClass, label@ SqlRexConvertlet { cx, call ->
            try {
                @SuppressWarnings("argument.type.incompatible") val result: RexNode = method.invoke(
                    this@ReflectiveConvertletTable,
                    cx, call.getOperator(), call
                ) as RexNode
                return@label requireNonNull(result) {
                    ("null result from " + method
                            + " for call " + call)
                }
            } catch (e: IllegalAccessException) {
                throw RuntimeException("while converting $call", e)
            } catch (e: InvocationTargetException) {
                throw RuntimeException("while converting $call", e)
            }
        } as SqlRexConvertlet?)
    }

    @Override
    @Nullable
    operator fun get(call: SqlCall): SqlRexConvertlet? {
        var convertlet: SqlRexConvertlet?
        val op: SqlOperator = call.getOperator()

        // Is there a convertlet for this operator
        // (e.g. SqlStdOperatorTable.PLUS)?
        convertlet = map[op] as SqlRexConvertlet?
        if (convertlet != null) {
            return convertlet
        }

        // Is there a convertlet for this class of operator
        // (e.g. SqlBinaryOperator)?
        var clazz: Class<*> = op.getClass()
        while (clazz != null) {
            convertlet = map[clazz] as SqlRexConvertlet?
            if (convertlet != null) {
                return convertlet
            }
            clazz = clazz.getSuperclass()
        }

        // Is there a convertlet for this class of expression
        // (e.g. SqlCall)?
        clazz = call.getClass()
        while (clazz != null) {
            convertlet = map[clazz] as SqlRexConvertlet?
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
    protected fun registerOp(
        op: SqlOperator?, convertlet: SqlRexConvertlet?
    ) {
        map.put(op, convertlet)
    }

    /**
     * Registers that one operator is an alias for another.
     *
     * @param alias  Operator which is alias
     * @param target Operator to translate calls to
     */
    protected fun addAlias(
        alias: SqlOperator, target: SqlOperator
    ) {
        map.put(
            alias, SqlRexConvertlet { cx, call ->
                Preconditions.checkArgument(
                    call.getOperator() === alias,
                    "call to wrong operator"
                )
                val newCall: SqlCall = target.createCall(SqlParserPos.ZERO, call.getOperandList())
                cx.convertExpression(newCall)
            } as SqlRexConvertlet?)
    }
}
