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
package org.apache.calcite.util

import java.util.List

/**
 * Utilities to make vanilla lists look like stacks.
 */
@Deprecated // to be removed before 2.0
object Stacks {
    /**
     * Returns the most recently added element in the stack. Throws if the
     * stack is empty.
     */
    fun <T> peek(stack: List<T>): T {
        return stack[stack.size() - 1]
    }

    /**
     * Returns the `n`th most recently added element in the stack.
     * Throws if the stack is empty.
     */
    fun <T> peek(n: Int, stack: List<T>): T {
        return stack[stack.size() - n - 1]
    }

    /**
     * Adds an element to the stack.
     */
    fun <T> push(stack: List<T>, element: T) {
        stack.add(element)
    }

    /**
     * Removes an element from the stack. Asserts of the element is not the
     * one last added; throws if the stack is empty.
     */
    fun <T> pop(stack: List<T>, element: T) {
        assert(stack[stack.size() - 1] === element)
        stack.remove(stack.size() - 1)
    }

    /**
     * Removes an element from the stack and returns it.
     *
     *
     * Throws if the stack is empty.
     */
    fun <T> pop(stack: List<T>): T {
        return stack.remove(stack.size() - 1)
    }
}
