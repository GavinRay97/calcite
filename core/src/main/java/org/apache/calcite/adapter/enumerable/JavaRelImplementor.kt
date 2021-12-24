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

import org.apache.calcite.DataContext

/**
 * Abstract base class for implementations of [RelImplementor]
 * that generate java code.
 */
abstract class JavaRelImplementor protected constructor(rexBuilder: RexBuilder) : RelImplementor {
    private val rexBuilder: RexBuilder

    init {
        this.rexBuilder = rexBuilder
        assert(rexBuilder.getTypeFactory() is JavaTypeFactory) { "Type factory of rexBuilder should be a JavaTypeFactory" }
    }

    fun getRexBuilder(): RexBuilder {
        return rexBuilder
    }

    val typeFactory: JavaTypeFactory
        get() = rexBuilder.getTypeFactory() as JavaTypeFactory

    /**
     * Returns the expression used to access
     * [org.apache.calcite.DataContext].
     *
     * @return expression used to access [org.apache.calcite.DataContext].
     */
    val rootExpression: ParameterExpression
        get() = DataContext.ROOT
}
