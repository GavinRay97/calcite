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
package org.apache.calcite.interpreter

import org.apache.calcite.DataContext

/**
 * Compiles a scalar expression ([RexNode]) to an expression that
 * can be evaluated ([Scalar]) by generating a Java AST and compiling it
 * to a class using Janino.
 */
class JaninoRexCompiler(rexBuilder: RexBuilder) : Interpreter.ScalarCompiler {
    private val rexBuilder: RexBuilder

    init {
        this.rexBuilder = rexBuilder
    }

    @Override
    fun compile(
        nodes: List<RexNode?>,
        inputRowType: RelDataType?
    ): Scalar.Producer {
        val programBuilder = RexProgramBuilder(inputRowType, rexBuilder)
        for (node in nodes) {
            programBuilder.addProject(node, null)
        }
        val program: RexProgram = programBuilder.getProgram()
        val list = BlockBuilder()
        val staticList: BlockBuilder = BlockBuilder().withRemoveUnused(false)
        val context_: ParameterExpression = Expressions.parameter(Context::class.java, "context")
        val outputValues_: ParameterExpression = Expressions.parameter(Array<Object>::class.java, "outputValues")
        val javaTypeFactory = JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem())

        // public void execute(Context, Object[] outputValues)
        val inputGetter: RexToLixTranslator.InputGetter = InputGetterImpl(
            Expressions.field(
                context_,
                BuiltInMethod.CONTEXT_VALUES.field
            ),
            PhysTypeImpl.of(
                javaTypeFactory, inputRowType,
                JavaRowFormat.ARRAY, false
            )
        )
        val correlates: Function1<String, RexToLixTranslator.InputGetter> =
            Function1<String, RexToLixTranslator.InputGetter> { a0 -> throw UnsupportedOperationException() }
        val root: Expression = Expressions.field(context_, BuiltInMethod.CONTEXT_ROOT.field)
        val conformance: SqlConformance = SqlConformanceEnum.DEFAULT // TODO: get this from implementor
        val expressionList: List<Expression> = RexToLixTranslator.translateProjects(
            program, javaTypeFactory,
            conformance, list, staticList, null, root, inputGetter, correlates
        )
        Ord.forEach(expressionList) { expression, i ->
            list.add(
                Expressions.statement(
                    Expressions.assign(
                        Expressions.arrayIndex(
                            outputValues_,
                            Expressions.constant(i)
                        ),
                        expression
                    )
                )
            )
        }
        return baz(
            context_, outputValues_, list.toBlock(),
            staticList.toBlock().statements
        )
    }

    companion object {
        /** Given a method that implements [Scalar.execute],
         * adds a bridge method that implements [Scalar.execute], and
         * compiles.  */
        fun baz(
            context_: ParameterExpression?,
            outputValues_: ParameterExpression?, block: BlockStatement?,
            declList: List<Statement?>?
        ): Scalar.Producer {
            val declarations: List<MemberDeclaration> = ArrayList()
            val innerDeclarations: List<MemberDeclaration> = ArrayList()

            // public Scalar apply(DataContext root) {
            //   <<staticList>>
            //   return new Scalar() {
            //     <<inner declarations>>
            //   };
            // }
            val statements: List<Statement> = ArrayList(declList)
            statements.add(
                Expressions.return_(
                    null,
                    Expressions.new_(
                        Scalar::class.java, ImmutableList.of(),
                        innerDeclarations
                    )
                )
            )
            declarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC, Scalar::class.java,
                    BuiltInMethod.FUNCTION_APPLY.method.getName(),
                    ImmutableList.of(DataContext.ROOT),
                    Expressions.block(statements)
                )
            )

            // (bridge method)
            // public Object apply(Object root) {
            //   return this.apply((DataContext) root);
            // }
            val objectRoot: ParameterExpression = Expressions.parameter(Object::class.java, "root")
            declarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC, Object::class.java,
                    BuiltInMethod.FUNCTION_APPLY.method.getName(),
                    ImmutableList.of(
                        objectRoot
                    ),
                    Expressions.block(
                        Expressions.return_(
                            null,
                            Expressions.call(
                                Expressions.parameter(Scalar.Producer::class.java, "this"),
                                BuiltInMethod.FUNCTION_APPLY.method,
                                Expressions.convert_(
                                    objectRoot,
                                    DataContext::class.java
                                )
                            )
                        )
                    )
                )
            )

            // public void execute(Context, Object[] outputValues)
            innerDeclarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC, Void.TYPE,
                    BuiltInMethod.SCALAR_EXECUTE2.method.getName(),
                    ImmutableList.of(context_, outputValues_), block
                )
            )

            // public Object execute(Context)
            val builder = BlockBuilder()
            val values_: Expression = builder.append(
                "values",
                Expressions.newArrayBounds(
                    Object::class.java, 1,
                    Expressions.constant(1)
                )
            )
            builder.add(
                Expressions.statement(
                    Expressions.call(
                        Expressions.parameter(Scalar::class.java, "this"),
                        BuiltInMethod.SCALAR_EXECUTE2.method, context_, values_
                    )
                )
            )
            builder.add(
                Expressions.return_(
                    null,
                    Expressions.arrayIndex(values_, Expressions.constant(0))
                )
            )
            innerDeclarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC, Object::class.java,
                    BuiltInMethod.SCALAR_EXECUTE1.method.getName(),
                    ImmutableList.of(context_), builder.toBlock()
                )
            )
            val classDeclaration: ClassDeclaration = Expressions.classDecl(
                Modifier.PUBLIC, "Buzz", null,
                ImmutableList.of(Scalar.Producer::class.java), declarations
            )
            val s: String = Expressions.toString(declarations, "\n", false)
            if (CalciteSystemProperty.DEBUG.value()) {
                Util.debugCode(System.out, s)
            }
            return try {
                getScalar(classDeclaration, s)
            } catch (e: CompileException) {
                throw RuntimeException(e)
            } catch (e: IOException) {
                throw RuntimeException(e)
            }
        }

        @Throws(CompileException::class, IOException::class)
        fun getScalar(expr: ClassDeclaration, s: String?): Scalar.Producer {
            val compilerFactory: ICompilerFactory
            val classLoader: ClassLoader =
                Objects.requireNonNull(JaninoRexCompiler::class.java.getClassLoader(), "classLoader")
            compilerFactory = try {
                CompilerFactoryFactory.getDefaultCompilerFactory(classLoader)
            } catch (e: Exception) {
                throw IllegalStateException(
                    "Unable to instantiate java compiler", e
                )
            }
            val cbe: IClassBodyEvaluator = compilerFactory.newClassBodyEvaluator()
            cbe.setClassName(expr.name)
            cbe.setImplementedInterfaces(arrayOf<Class>(Scalar.Producer::class.java))
            cbe.setParentClassLoader(classLoader)
            if (CalciteSystemProperty.DEBUG.value()) {
                // Add line numbers to the generated janino class
                cbe.setDebuggingInformation(true, true, true)
            }
            return cbe.createInstance(StringReader(s)) as Scalar.Producer
        }
    }
}
