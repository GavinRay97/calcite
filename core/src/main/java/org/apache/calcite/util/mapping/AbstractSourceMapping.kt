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
 * Simple implementation of
 * [org.apache.calcite.util.mapping.Mappings.TargetMapping] where the
 * number of sources and targets are specified as constructor parameters, and you
 * just need to implement one method.
 */
abstract class AbstractSourceMapping protected constructor(
    private override val sourceCount: Int,
    private override val targetCount: Int
) : Mappings.AbstractMapping(), Mapping {
    @Override
    fun getSourceCount(): Int {
        return sourceCount
    }

    @Override
    fun getTargetCount(): Int {
        return targetCount
    }

    @Override
    override fun inverse(): Mapping {
        return Mappings.invert(this)
    }

    @Override
    override fun size(): Int {
        return targetCount
    }

    @Override
    override fun clear() {
        throw UnsupportedOperationException()
    }

    @Override
    fun getMappingType(): MappingType {
        return MappingType.INVERSE_PARTIAL_FUNCTION
    }

    @SuppressWarnings("method.invocation.invalid")
    @Override
    override operator fun iterator(): Iterator<IntPair> {
        return object : Iterator<IntPair?>() {
            var source = 0
            var target = -1

            init {
                moveToNext()
            }

            private fun moveToNext() {
                while (++target < targetCount) {
                    source = getSourceOpt(target)
                    if (source >= 0) {
                        break
                    }
                }
            }

            @Override
            override fun hasNext(): Boolean {
                return target < targetCount
            }

            @Override
            override fun next(): IntPair {
                val p = IntPair(source, target)
                moveToNext()
                return p
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException("remove")
            }
        }
    }

    @Override
    abstract override fun getSourceOpt(source: Int): Int
}
