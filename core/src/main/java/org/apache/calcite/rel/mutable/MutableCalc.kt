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

import org.apache.calcite.rex.RexProgram

/** Mutable equivalent of [org.apache.calcite.rel.core.Calc].  */
class MutableCalc private constructor(input: MutableRel, program: RexProgram) :
    MutableSingleRel(MutableRelType.CALC, program.getOutputRowType(), input) {
    val program: RexProgram

    init {
        this.program = program
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableCalc
                && MutableRel.STRING_EQUIVALENCE.equivalent(
            program, (obj as MutableCalc).program
        )
                && input!!.equals((obj as MutableCalc).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            input,
            MutableRel.STRING_EQUIVALENCE.hash(program)
        )
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Calc(program: ").append(program).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(input!!.clone(), program)
    }

    companion object {
        /**
         * Creates a MutableCalc.
         *
         * @param input   Input relational expression
         * @param program Calc program
         */
        fun of(input: MutableRel, program: RexProgram): MutableCalc {
            return MutableCalc(input, program)
        }
    }
}
