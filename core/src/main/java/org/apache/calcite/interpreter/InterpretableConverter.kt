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
 * Relational expression that converts any relational expression input to
 * [org.apache.calcite.interpreter.InterpretableConvention], by wrapping
 * it in an interpreter.
 */
class InterpretableConverter protected constructor(
    cluster: RelOptCluster?, traits: RelTraitSet?,
    input: RelNode?
) : ConverterImpl(cluster, ConventionTraitDef.INSTANCE, traits, input), ArrayBindable {
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return InterpretableConverter(getCluster(), traitSet, sole(inputs))
    }

    @get:Override
    val elementType: Class<Array<Object>>
        get() = Array<Object>::class.java

    @Override
    fun bind(dataContext: DataContext?): Enumerable<Array<Object>> {
        return Interpreter(dataContext, getInput())
    }
}
