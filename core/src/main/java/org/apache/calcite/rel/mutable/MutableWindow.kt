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
package org.apache.calcite.rel.mutable

import org.apache.calcite.rel.core.Window.Group

/** Mutable equivalent of [org.apache.calcite.rel.core.Window].  */
class MutableWindow private constructor(
    rowType: RelDataType, input: MutableRel,
    groups: List<Group>, constants: List<RexLiteral>
) : MutableSingleRel(MutableRelType.WINDOW, rowType, input) {
    val groups: List<Group>
    val constants: List<RexLiteral>

    init {
        this.groups = groups
        this.constants = constants
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableWindow
                && groups.equals((obj as MutableWindow).groups)
                && constants.equals((obj as MutableWindow).constants)
                && input!!.equals((obj as MutableWindow).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, groups, constants)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Window(groups: ").append(groups)
            .append(", constants: ").append(constants).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(rowType, input!!.clone(), groups, constants)
    }

    companion object {
        /**
         * Creates a MutableWindow.
         *
         * @param rowType   Row type
         * @param input     Input relational expression
         * @param groups    Window groups
         * @param constants List of constants that are additional inputs
         */
        fun of(
            rowType: RelDataType,
            input: MutableRel, groups: List<Group>, constants: List<RexLiteral>
        ): MutableWindow {
            return MutableWindow(rowType, input, groups, constants)
        }
    }
}
