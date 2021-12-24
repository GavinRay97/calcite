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

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.function.Function1
import org.apache.calcite.runtime.Hook
import org.apache.calcite.runtime.Utilities
import org.apache.calcite.util.Pair
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.ClassBodyEvaluator
import org.codehaus.janino.Scanner
import java.io.IOException
import java.io.Serializable
import java.io.StringReader
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.util.Arrays
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Result of compiling code generated from a [RexNode] expression.
 */
class RexExecutable(val source: String, reason: Object) {
    private val compiledFunction: Function1<DataContext, Array<Object>>

    @Nullable
    private var dataContext: DataContext? = null

    init {
        compiledFunction = compile(source, reason)
    }

    fun setDataContext(dataContext: DataContext?) {
        this.dataContext = dataContext
    }

    fun reduce(
        rexBuilder: RexBuilder, constExps: List<RexNode?>,
        reducedValues: List<RexNode?>
    ) {
        @Nullable var values: Array<Object?>?
        try {
            values = execute()
            if (values == null) {
                reducedValues.addAll(constExps)
                values = arrayOfNulls<Object>(constExps.size())
            } else {
                assert(values.size == constExps.size())
                val valueList: List<Object> = Arrays.asList(values)
                for (value in Pair.zip(constExps, valueList)) {
                    reducedValues.add(
                        rexBuilder.makeLiteral(value.right, value.left.getType(), true)
                    )
                }
            }
        } catch (e: RuntimeException) {
            // One or more of the expressions failed.
            // Don't reduce any of the expressions.
            reducedValues.addAll(constExps)
            values = arrayOfNulls<Object>(constExps.size())
        }
        Hook.EXPRESSION_REDUCER.run(Pair.of(source, values))
    }

    val function: (Any) -> Array<Any>
        get() = compiledFunction

    @Nullable
    fun execute(): @Nullable Array<Object?>? {
        return compiledFunction.apply(requireNonNull(dataContext, "dataContext"))
    }

    companion object {
        private const val GENERATED_CLASS_NAME = "Reducer"
        private fun compile(
            code: String,
            reason: Object
        ): Function1<DataContext, Array<Object>> {
            return try {
                val cbe = ClassBodyEvaluator()
                cbe.setClassName(GENERATED_CLASS_NAME)
                cbe.setExtendedClass(Utilities::class.java)
                cbe.setImplementedInterfaces(arrayOf<Class>(Function1::class.java, Serializable::class.java))
                cbe.setParentClassLoader(RexExecutable::class.java.getClassLoader())
                cbe.cook(Scanner(null, StringReader(code)))
                val c: Class = cbe.getClazz()
                val constructor: Constructor<Function1<DataContext, Array<Object>>> = c.getConstructor()
                constructor.newInstance()
            } catch (e: CompileException) {
                throw RuntimeException("While compiling $reason", e)
            } catch (e: IOException) {
                throw RuntimeException("While compiling $reason", e)
            } catch (e: InstantiationException) {
                throw RuntimeException("While compiling $reason", e)
            } catch (e: IllegalAccessException) {
                throw RuntimeException("While compiling $reason", e)
            } catch (e: InvocationTargetException) {
                throw RuntimeException("While compiling $reason", e)
            } catch (e: NoSuchMethodException) {
                throw RuntimeException("While compiling $reason", e)
            }
        }
    }
}
