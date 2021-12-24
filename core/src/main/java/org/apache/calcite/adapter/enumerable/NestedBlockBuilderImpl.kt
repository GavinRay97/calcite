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

import org.apache.calcite.linq4j.tree.BlockBuilder

/**
 * Allows to build nested code blocks with tracking of current context.
 *
 * @see org.apache.calcite.adapter.enumerable.StrictAggImplementor.implementAdd
 */
class NestedBlockBuilderImpl @SuppressWarnings("method.invocation.invalid") constructor(block: BlockBuilder?) :
    NestedBlockBuilder {
    private val blocks: List<BlockBuilder> = ArrayList()

    /**
     * Constructs nested block builders starting of a given code block.
     * @param block root code block
     */
    init {
        nestBlock(block)
    }

    /**
     * Starts nested code block. The resulting block can optimize expressions
     * and reuse already calculated values from the parent blocks.
     * @return new code block that can optimize expressions and reuse already
     * calculated values from the parent blocks.
     */
    @Override
    override fun nestBlock(): BlockBuilder {
        val block = BlockBuilder(true, currentBlock())
        nestBlock(block)
        return block
    }

    /**
     * Uses given block as the new code context.
     * The current block will be restored after [.exitBlock] call.
     * @param block new code block
     * @see .exitBlock
     */
    @Override
    override fun nestBlock(block: BlockBuilder?) {
        blocks.add(block)
    }

    /**
     * Returns the current code block.
     */
    @Override
    override fun currentBlock(): BlockBuilder {
        return blocks[blocks.size() - 1]
    }

    /**
     * Leaves the current code block.
     * @see .nestBlock
     */
    @Override
    override fun exitBlock() {
        blocks.remove(blocks.size() - 1)
    }
}
