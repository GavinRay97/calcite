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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.function.Experimental

/**
 * Relational expression that iterates over its input and, in addition to
 * returning its results, will forward them into other consumers.
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
abstract class Spool protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
    readType: Type?, writeType: Type?
) : SingleRel(cluster, traitSet, input) {
    /**
     * Enumeration representing spool read / write type.
     */
    enum class Type {
        EAGER, LAZY
    }

    /**
     * How the spool consumes elements from its input.
     *
     *
     *  * EAGER: the spool consumes the elements from its input at once at the
     * initial request;
     *  * LAZY: the spool consumes the elements from its input one by one by
     * request.
     *
     */
    val readType: Type

    /**
     * How the spool forwards elements to consumers.
     *
     *
     *  * EAGER: the spool forwards each element as soon as it returns it;
     *  * LAZY: the spool forwards all elements at once when it is done returning
     * all of them.
     *
     */
    val writeType: Type
    //~ Constructors -----------------------------------------------------------
    /** Creates a Spool.  */
    init {
        this.readType = Objects.requireNonNull(readType, "readType")
        this.writeType = Objects.requireNonNull(writeType, "writeType")
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): RelNode {
        return copy(traitSet, sole(inputs), readType, writeType)
    }

    protected abstract fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        readType: Type?, writeType: Type?
    ): Spool

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("readType", readType)
            .item("writeType", writeType)
    }
}
