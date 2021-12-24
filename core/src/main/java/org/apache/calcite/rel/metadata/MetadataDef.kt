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
package org.apache.calcite.rel.metadata

import org.apache.calcite.rel.RelNode

/**
 * Definition of metadata.
 *
 * @param <M> Kind of metadata
</M> */
class MetadataDef<M : Metadata?> private constructor(
    metadataClass: Class<M>,
    handlerClass: Class<out MetadataHandler<M>?>, vararg methods: Method
) {
    val metadataClass: Class<M>
    val handlerClass: Class<out MetadataHandler<M>?>
    val methods: ImmutableList<Method>

    init {
        this.metadataClass = metadataClass
        this.handlerClass = handlerClass
        this.methods = ImmutableList.copyOf(methods)
        val handlerMethods: Array<Method> = Arrays.stream(handlerClass.getDeclaredMethods())
            .filter { m -> !m.getName().equals("getDef") }.toArray { i -> arrayOfNulls<Method>(i) }
        assert(handlerMethods.size == methods.size)
        for (pair in Pair.zip(methods, handlerMethods)) {
            val leftTypes: List<Class<*>> = Arrays.asList(pair.left.getParameterTypes())
            val rightTypes: List<Class<*>> = Arrays.asList(pair.right.getParameterTypes())
            assert(leftTypes.size() + 2 === rightTypes.size())
            assert(RelNode::class.java.isAssignableFrom(rightTypes[0]))
            assert(RelMetadataQuery::class.java === rightTypes[1])
            assert(leftTypes.equals(rightTypes.subList(2, rightTypes.size())))
        }
    }

    companion object {
        /** Creates a [org.apache.calcite.rel.metadata.MetadataDef].  */
        fun <M : Metadata?> of(
            metadataClass: Class<M>,
            handlerClass: Class<out MetadataHandler<M>?>, vararg methods: Method?
        ): MetadataDef<M> {
            return MetadataDef(metadataClass, handlerClass, *methods)
        }
    }
}
