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
package org.apache.calcite.runtime

import org.apache.calcite.DataContext
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.FunctionContext

/**
 * Runtime support for [org.apache.calcite.schema.FunctionContext].
 */
object FunctionContexts {
    /** Creates a FunctionContext.  */
    fun of(root: DataContext, argumentValues: Array<Object?>): FunctionContext {
        return FunctionContextImpl(root, argumentValues)
    }

    /** Implementation of [org.apache.calcite.schema.FunctionContext].  */
    private class FunctionContextImpl internal constructor(
        root: DataContext,
        @Nullable argumentValues: Array<Object?>
    ) : FunctionContext {
        private val root: DataContext

        @Nullable
        private val argumentValues: Array<Object?>

        init {
            this.root = root
            this.argumentValues = argumentValues
        }

        @get:Override
        val typeFactory: RelDataTypeFactory
            get() = root.getTypeFactory()

        @Override
        fun getParameterCount(): Int {
            return argumentValues.size
        }

        @Override
        fun isArgumentConstant(ordinal: Int): Boolean {
            return argumentValue(ordinal) != null
        }

        @Nullable
        private fun argumentValue(ordinal: Int): Object? {
            if (ordinal < 0 || ordinal >= argumentValues.size) {
                throw IndexOutOfBoundsException(
                    "argument ordinal " + ordinal
                            + " is out of range"
                )
            }
            return argumentValues[ordinal]
        }

        @Override
        fun <V> getArgumentValueAs(
            ordinal: Int,
            valueClass: Class<V>
        ): @Nullable V? {
            val v: Object = argumentValue(ordinal)
                ?: throw IllegalArgumentException(
                    "value of argument " + ordinal
                            + " is not constant"
                )
            if (v === NullSentinel.INSTANCE) {
                return null // value is constant NULL
            }
            return if (valueClass === String::class.java
                && v !is String
            ) {
                valueClass.cast(v.toString())
            } else valueClass.cast(v)
        }
    }
}
