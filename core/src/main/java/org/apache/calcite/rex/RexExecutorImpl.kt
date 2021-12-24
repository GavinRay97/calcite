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
package org.apache.calcite.rex

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable.EnumUtils
import org.apache.calcite.adapter.enumerable.RexToLixTranslator
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.linq4j.tree.BlockBuilder
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.linq4j.tree.IndexExpression
import org.apache.calcite.linq4j.tree.MethodCallExpression
import org.apache.calcite.linq4j.tree.MethodDeclaration
import org.apache.calcite.linq4j.tree.ParameterExpression
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.validate.SqlConformance
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.apache.calcite.util.BuiltInMethod
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import java.lang.reflect.Modifier
import java.lang.reflect.Type
import java.util.List

/**
 * Evaluates a [RexNode] expression.
 *
 *
 * For this impl, all the public methods should be
 * static except that it inherits from [RexExecutor].
 * This pretends that other code in the project assumes
 * the executor instance is [RexExecutorImpl].
 */
class RexExecutorImpl(dataContext: DataContext) : RexExecutor {
    private val dataContext: DataContext

    init {
        this.dataContext = dataContext
    }

    /**
     * Do constant reduction using generated code.
     */
    @Override
    fun reduce(
        rexBuilder: RexBuilder, constExps: List<RexNode>,
        reducedValues: List<RexNode?>
    ) {
        val code = compile(rexBuilder, constExps,
            RexToLixTranslator.InputGetter { list, index, storageType -> throw UnsupportedOperationException() })
        val executable = RexExecutable(code, constExps)
        executable.setDataContext(dataContext)
        executable.reduce(rexBuilder, constExps, reducedValues)
    }

    /**
     * Implementation of
     * [org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter]
     * that reads the values of input fields by calling
     * `[org.apache.calcite.DataContext.get]("inputRecord")`.
     */
    private class DataContextInputGetter internal constructor(
        rowType: RelDataType,
        typeFactory: RelDataTypeFactory
    ) : InputGetter {
        private val typeFactory: RelDataTypeFactory
        private val rowType: RelDataType

        init {
            this.rowType = rowType
            this.typeFactory = typeFactory
        }

        @Override
        fun field(list: BlockBuilder?, index: Int, @Nullable storageType: Type?): Expression {
            var storageType: Type? = storageType
            val recFromCtx: MethodCallExpression = Expressions.call(
                DataContext.ROOT,
                BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant("inputRecord")
            )
            val recFromCtxCasted: Expression = EnumUtils.convert(recFromCtx, Array<Object>::class.java)
            val recordAccess: IndexExpression = Expressions.arrayIndex(
                recFromCtxCasted,
                Expressions.constant(index)
            )
            if (storageType == null) {
                val fieldType: RelDataType = rowType.getFieldList().get(index).getType()
                storageType = (typeFactory as JavaTypeFactory).getJavaClass(fieldType)
            }
            return EnumUtils.convert(recordAccess, storageType)
        }
    }

    companion object {
        private fun compile(
            rexBuilder: RexBuilder, constExps: List<RexNode>,
            getter: RexToLixTranslator.InputGetter
        ): String {
            val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
            val emptyRowType: RelDataType = typeFactory.builder().build()
            return compile(rexBuilder, constExps, getter, emptyRowType)
        }

        private fun compile(
            rexBuilder: RexBuilder, constExps: List<RexNode>,
            getter: RexToLixTranslator.InputGetter, rowType: RelDataType
        ): String {
            val programBuilder = RexProgramBuilder(rowType, rexBuilder)
            for (node in constExps) {
                programBuilder.addProject(
                    node, "c" + programBuilder.getProjectList().size()
                )
            }
            val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
            val javaTypeFactory: JavaTypeFactory =
                if (typeFactory is JavaTypeFactory) typeFactory as JavaTypeFactory else JavaTypeFactoryImpl(typeFactory.getTypeSystem())
            val blockBuilder = BlockBuilder()
            val root0_: ParameterExpression = Expressions.parameter(Object::class.java, "root0")
            val root_: ParameterExpression = DataContext.ROOT
            blockBuilder.add(
                Expressions.declare(
                    Modifier.FINAL, root_,
                    Expressions.convert_(root0_, DataContext::class.java)
                )
            )
            val conformance: SqlConformance = SqlConformanceEnum.DEFAULT
            val program: RexProgram = programBuilder.getProgram()
            val expressions: List<Expression> = RexToLixTranslator.translateProjects(
                program, javaTypeFactory,
                conformance, blockBuilder, null, null, root_, getter, null
            )
            blockBuilder.add(
                Expressions.return_(
                    null,
                    Expressions.newArrayInit(Array<Object>::class.java, expressions)
                )
            )
            val methodDecl: MethodDeclaration = Expressions.methodDecl(
                Modifier.PUBLIC, Array<Object>::class.java,
                BuiltInMethod.FUNCTION1_APPLY.method.getName(),
                ImmutableList.of(root0_), blockBuilder.toBlock()
            )
            val code: String = Expressions.toString(methodDecl)
            if (CalciteSystemProperty.DEBUG.value()) {
                Util.debugCode(System.out, code)
            }
            return code
        }

        /**
         * Creates an [RexExecutable] that allows to apply the
         * generated code during query processing (filter, projection).
         *
         * @param rexBuilder Rex builder
         * @param exps Expressions
         * @param rowType describes the structure of the input row.
         */
        fun getExecutable(
            rexBuilder: RexBuilder, exps: List<RexNode>,
            rowType: RelDataType
        ): RexExecutable {
            val typeFactory = JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem())
            val getter: InputGetter = DataContextInputGetter(rowType, typeFactory)
            val code = compile(rexBuilder, exps, getter, rowType)
            return RexExecutable(code, "generated Rex code")
        }
    }
}
