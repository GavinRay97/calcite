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
 * The base implementation of strict aggregate function.
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.CountImplementor
 *
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.SumImplementor
 */
abstract class StrictAggImplementor : AggImplementor {
    private var needTrackEmptySet = false
    private var trackNullsPerRow = false
    protected var stateSize = 0
        private set

    protected fun nonDefaultOnEmptySet(info: AggContext): Boolean {
        return info.returnRelType().isNullable()
    }

    @Override
    fun getStateType(info: AggContext): List<Type> {
        val subState: List<Type> = getNotNullState(info)
        stateSize = subState.size()
        needTrackEmptySet = nonDefaultOnEmptySet(info)
        if (!needTrackEmptySet) {
            return subState
        }
        val hasNullableArgs = anyNullable(info.parameterRelTypes())
        trackNullsPerRow = info !is WinAggContext || hasNullableArgs
        val res: List<Type> = ArrayList(subState.size() + 1)
        res.addAll(subState)
        res.add(Boolean::class.javaPrimitiveType) // has not nulls
        return res
    }

    fun getNotNullState(info: AggContext): List<Type> {
        var type: Type = info.returnType()
        type = EnumUtils.fromInternal(type)
        type = Primitive.unbox(type)
        return Collections.singletonList(type)
    }

    @Override
    fun implementReset(info: AggContext?, reset: AggResetContext) {
        if (trackNullsPerRow) {
            val acc: List<Expression> = reset.accumulator()
            val flag: Expression = acc[acc.size() - 1]
            val block: BlockBuilder = reset.currentBlock()
            block.add(
                Expressions.statement(
                    Expressions.assign(
                        flag,
                        RexImpTable.getDefaultValue(flag.getType())
                    )
                )
            )
        }
        implementNotNullReset(info, reset)
    }

    protected fun implementNotNullReset(
        info: AggContext?,
        reset: AggResetContext
    ) {
        val block: BlockBuilder = reset.currentBlock()
        val accumulator: List<Expression> = reset.accumulator()
        for (i in 0 until stateSize) {
            val exp: Expression = accumulator[i]
            block.add(
                Expressions.statement(
                    Expressions.assign(
                        exp,
                        RexImpTable.getDefaultValue(exp.getType())
                    )
                )
            )
        }
    }

    @Override
    fun implementAdd(info: AggContext?, add: AggAddContext) {
        val args: List<RexNode> = add.rexArguments()
        val translator: RexToLixTranslator = add.rowTranslator()
        val conditions: List<Expression> = ArrayList()
        conditions.addAll(
            translator.translateList(args, RexImpTable.NullAs.IS_NOT_NULL)
        )
        val filterArgument: RexNode = add.rexFilterArgument()
        if (filterArgument != null) {
            conditions.add(
                translator.translate(
                    filterArgument,
                    RexImpTable.NullAs.FALSE
                )
            )
        }
        val condition: Expression = Expressions.foldAnd(conditions)
        if (Expressions.constant(false).equals(condition)) {
            return
        }
        val argsNotNull: Boolean = Expressions.constant(true).equals(condition)
        val thenBlock: BlockBuilder = if (argsNotNull) add.currentBlock() else BlockBuilder(true, add.currentBlock())
        if (trackNullsPerRow) {
            val acc: List<Expression> = add.accumulator()
            thenBlock.add(
                Expressions.statement(
                    Expressions.assign(
                        acc[acc.size() - 1],
                        Expressions.constant(true)
                    )
                )
            )
        }
        if (argsNotNull) {
            implementNotNullAdd(info, add)
            return
        }
        add.nestBlock(thenBlock)
        implementNotNullAdd(info, add)
        add.exitBlock()
        add.currentBlock().add(Expressions.ifThen(condition, thenBlock.toBlock()))
    }

    protected abstract fun implementNotNullAdd(
        info: AggContext?,
        add: AggAddContext?
    )

    @Override
    fun implementResult(
        info: AggContext,
        result: AggResultContext
    ): Expression? {
        if (!needTrackEmptySet) {
            return EnumUtils.convert(
                implementNotNullResult(info, result), info.returnType()
            )
        }
        val tmpName = if (result.accumulator().isEmpty()) "ar" else result.accumulator().get(0) + "\$Res"
        val res: ParameterExpression = Expressions.parameter(
            0, info.returnType(),
            result.currentBlock().newName(tmpName)
        )
        val acc: List<Expression> = result.accumulator()
        val thenBlock: BlockBuilder = result.nestBlock()
        val nonNull: Expression = EnumUtils.convert(
            implementNotNullResult(info, result), info.returnType()
        )
        result.exitBlock()
        thenBlock.add(Expressions.statement(Expressions.assign(res, nonNull)))
        val thenBranch: BlockStatement = thenBlock.toBlock()
        val seenNotNullRows: Expression =
            if (trackNullsPerRow) acc[acc.size() - 1] else (result as WinAggResultContext).hasRows()
        if (thenBranch.statements.size() === 1) {
            return Expressions.condition(
                seenNotNullRows,
                nonNull, RexImpTable.getDefaultValue(res.getType())
            )
        }
        result.currentBlock().add(Expressions.declare(0, res, null))
        result.currentBlock().add(
            Expressions.ifThenElse(
                seenNotNullRows,
                thenBranch,
                Expressions.statement(
                    Expressions.assign(
                        res,
                        RexImpTable.getDefaultValue(res.getType())
                    )
                )
            )
        )
        return res
    }

    protected fun implementNotNullResult(
        info: AggContext?,
        result: AggResultContext
    ): Expression {
        return result.accumulator().get(0)
    }

    companion object {
        protected fun accAdvance(
            add: AggAddContext, acc: Expression,
            next: Expression?
        ) {
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(acc, EnumUtils.convert(next, acc.type))
                )
            )
        }

        private fun anyNullable(types: List<RelDataType?>): Boolean {
            for (type in types) {
                if (type.isNullable()) {
                    return true
                }
            }
            return false
        }
    }
}
