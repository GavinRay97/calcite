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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.linq4j.tree.Expression

/**
 * Implementation of
 * [org.apache.calcite.adapter.enumerable.NotNullImplementor]
 * that calls a given [java.lang.reflect.Method].
 *
 *
 * When method is not static, a new instance of the required class is
 * created.
 */
class ReflectiveCallNotNullImplementor(method: Method) : NotNullImplementor {
    protected val method: Method

    /**
     * Constructor of [ReflectiveCallNotNullImplementor].
     *
     * @param method Method that is used to implement the call
     */
    init {
        this.method = method
    }

    @Override
    fun implement(
        translator: RexToLixTranslator,
        call: RexCall?, translatedOperands: List<Expression?>
    ): Expression {
        var translatedOperands: List<Expression?> = translatedOperands
        translatedOperands = EnumUtils.fromInternal(method.getParameterTypes(), translatedOperands)
        translatedOperands = EnumUtils.convertAssignableTypes(method.getParameterTypes(), translatedOperands)
        val callExpr: Expression
        callExpr = if (method.getModifiers() and Modifier.STATIC !== 0) {
            Expressions.call(method, translatedOperands)
        } else {
            val target: Expression = translator.functionInstance(call, method)
            Expressions.call(target, method, translatedOperands)
        }
        return if (!containsCheckedException(
                method
            )
        ) {
            callExpr
        } else translator.handleMethodCheckedExceptions(callExpr)
    }

    companion object {
        private fun containsCheckedException(method: Method): Boolean {
            val exceptions: Array<Class> = method.getExceptionTypes()
            if (exceptions == null || exceptions.size == 0) {
                return false
            }
            for (clazz in exceptions) {
                if (!RuntimeException::class.java.isAssignableFrom(clazz)) {
                    return true
                }
            }
            return false
        }
    }
}
