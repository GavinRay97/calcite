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
package org.apache.calcite.interpreter

import org.apache.calcite.DataContext

/**
 * Utilities relating to [org.apache.calcite.interpreter.Interpreter]
 * and [org.apache.calcite.interpreter.InterpretableConvention].
 */
object Interpreters {
    /** Creates a [org.apache.calcite.runtime.Bindable] that interprets a
     * given relational expression.  */
    fun bindable(rel: RelNode): ArrayBindable {
        return if (rel is ArrayBindable) {
            // E.g. if rel instanceof BindableRel
            rel as ArrayBindable
        } else object : ArrayBindable() {
            @Override
            fun bind(dataContext: DataContext?): Enumerable<Array<Object>> {
                return Interpreter(dataContext, rel)
            }

            @get:Override
            val elementType: Class<Array<Object>>
                get() = Array<Object>::class.java
        }
    }
}
