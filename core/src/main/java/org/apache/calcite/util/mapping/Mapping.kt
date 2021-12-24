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
package org.apache.calcite.util.mapping

import java.util.Iterator

/**
 * A <dfn>Mapping</dfn> is a relationship between a source domain to target
 * domain of integers.
 *
 *
 * This interface represents the most general possible mapping. Depending on
 * the [MappingType] of a particular mapping, some of the operations may
 * not be applicable. If you call the method, you will receive a runtime error.
 * For instance:
 *
 *
 *  * If a target has more than one source, then the method
 * [.getSource] will throw
 * [Mappings.TooManyElementsException].
 *  * If a source has no targets, then the method [.getTarget] will throw
 * [Mappings.NoElementException].
 *
 */
interface Mapping : Mappings.FunctionMapping, Mappings.SourceMapping, Mappings.TargetMapping, Iterable<IntPair?> {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns an iterator over the elements in this mapping.
     *
     *
     * This method is optional; implementations may throw
     * [UnsupportedOperationException].
     */
    @Override
    override fun iterator(): Iterator<IntPair>

    /**
     * Returns the number of sources. Valid sources will be in the range 0 ..
     * sourceCount.
     */
    @get:Override
    override val sourceCount: Int

    /**
     * Returns the number of targets. Valid targets will be in the range 0 ..
     * targetCount.
     */
    @get:Override
    override val targetCount: Int

    @get:Override
    override val mappingType: org.apache.calcite.util.mapping.MappingType?

    /**
     * Returns whether this mapping is the identity.
     */
    @get:Override
    override val isIdentity: Boolean

    /**
     * Removes all elements in the mapping.
     */
    fun clear()

    /**
     * Returns the number of elements in the mapping.
     */
    @Override
    override fun size(): Int
}
