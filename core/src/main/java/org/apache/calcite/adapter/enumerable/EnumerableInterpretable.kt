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

import org.apache.calcite.DataContext

/**
 * Relational expression that converts an enumerable input to interpretable
 * calling convention.
 *
 * @see EnumerableConvention
 *
 * @see org.apache.calcite.interpreter.BindableConvention
 */
class EnumerableInterpretable protected constructor(cluster: RelOptCluster, input: RelNode?) : ConverterImpl(
    cluster, ConventionTraitDef.INSTANCE,
    cluster.traitSetOf(InterpretableConvention.INSTANCE), input
), InterpretableRel {
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): EnumerableInterpretable {
        return EnumerableInterpretable(getCluster(), sole(inputs))
    }

    @Override
    fun implement(implementor: InterpreterImplementor): Node {
        val bindable: Bindable = toBindable(
            implementor.internalParameters,
            implementor.spark, getInput() as EnumerableRel,
            EnumerableRel.Prefer.ARRAY
        )
        val arrayBindable: ArrayBindable = box(bindable)
        val enumerable: Enumerable<Array<Object?>?> = arrayBindable.bind(implementor.dataContext)
        return EnumerableNode(enumerable, implementor.compiler, this)
    }

    /**
     * A visitor detecting if the Java AST contains static fields.
     */
    internal class StaticFieldDetector : VisitorImpl<Void?>() {
        var containsStaticField = false
        @Override
        fun visit(fieldDeclaration: FieldDeclaration): Void? {
            containsStaticField = fieldDeclaration.modifier and Modifier.STATIC !== 0
            return if (containsStaticField) null else super.visit(fieldDeclaration)
        }
    }

    /** Interpreter node that reads from an [Enumerable].
     *
     *
     * From the interpreter's perspective, it is a leaf node.  */
    private class EnumerableNode internal constructor(
        enumerable: Enumerable<Array<Object?>?>, compiler: Compiler,
        rel: EnumerableInterpretable?
    ) : Node {
        private val enumerable: Enumerable<Array<Object>>
        private val sink: Sink

        init {
            this.enumerable = enumerable
            sink = compiler.sink(rel)
        }

        @Override
        @Throws(InterruptedException::class)
        fun run() {
            val enumerator: Enumerator<Array<Object>> = enumerable.enumerator()
            while (enumerator.moveNext()) {
                @Nullable val values: Array<Object> = enumerator.current()
                sink.send(Row.of(values))
            }
        }
    }

    companion object {
        /**
         * The cache storing Bindable objects, instantiated via dynamically generated Java classes.
         *
         *
         * It allows to re-use Bindable objects for queries appearing relatively
         * often. It is used to avoid the cost of compiling and generating a new class
         * and also instantiating the object.
         */
        private val BINDABLE_CACHE: Cache<String, Bindable> = CacheBuilder.newBuilder()
            .concurrencyLevel(CalciteSystemProperty.BINDABLE_CACHE_CONCURRENCY_LEVEL.value())
            .maximumSize(CalciteSystemProperty.BINDABLE_CACHE_MAX_SIZE.value())
            .build()

        fun toBindable(
            parameters: Map<String?, Object?>,
            spark: @Nullable CalcitePrepare.SparkHandler?, rel: EnumerableRel,
            prefer: EnumerableRel.Prefer?
        ): Bindable {
            val relImplementor = EnumerableRelImplementor(
                rel.getCluster().getRexBuilder(),
                parameters
            )
            val expr: ClassDeclaration = relImplementor.implementRoot(rel, prefer)
            val s: String = Expressions.toString(expr.memberDeclarations, "\n", false)
            if (CalciteSystemProperty.DEBUG.value()) {
                Util.debugCode(System.out, s)
            }
            Hook.JAVA_PLAN.run(s)
            return try {
                if (spark != null && spark.enabled()) {
                    spark.compile(expr, s)
                } else {
                    getBindable(expr, s, rel.getRowType().getFieldCount())
                }
            } catch (e: Exception) {
                throw Helper.INSTANCE.wrap(
                    """
    Error while compiling generated Java code:
    $s
    """.trimIndent(), e
                )
            }
        }

        @Throws(CompileException::class, IOException::class, ExecutionException::class)
        fun getBindable(expr: ClassDeclaration, s: String?, fieldCount: Int): Bindable {
            val compilerFactory: ICompilerFactory
            val classLoader: ClassLoader =
                Objects.requireNonNull(EnumerableInterpretable::class.java.getClassLoader(), "classLoader")
            compilerFactory = try {
                CompilerFactoryFactory.getDefaultCompilerFactory(classLoader)
            } catch (e: Exception) {
                throw IllegalStateException(
                    "Unable to instantiate java compiler", e
                )
            }
            val cbe: IClassBodyEvaluator = compilerFactory.newClassBodyEvaluator()
            cbe.setClassName(expr.name)
            cbe.setExtendedClass(Utilities::class.java)
            cbe.setImplementedInterfaces(
                if (fieldCount == 1) arrayOf<Class>(Bindable::class.java, Typed::class.java) else arrayOf<Class>(
                    ArrayBindable::class.java
                )
            )
            cbe.setParentClassLoader(classLoader)
            if (CalciteSystemProperty.DEBUG.value()) {
                // Add line numbers to the generated janino class
                cbe.setDebuggingInformation(true, true, true)
            }
            if (CalciteSystemProperty.BINDABLE_CACHE_MAX_SIZE.value() !== 0) {
                val detector = StaticFieldDetector()
                expr.accept(detector)
                if (!detector.containsStaticField) {
                    return BINDABLE_CACHE.get(s) { cbe.createInstance(StringReader(s)) as Bindable }
                }
            }
            return cbe.createInstance(StringReader(s)) as Bindable
        }

        /** Converts a bindable over scalar values into an array bindable, with each
         * row as an array of 1 element.  */
        fun box(bindable: Bindable): ArrayBindable {
            return if (bindable is ArrayBindable) {
                bindable as ArrayBindable
            } else object : ArrayBindable() {
                @get:Override
                val elementType: Class<Array<Object>>
                    get() = Array<Object>::class.java

                @Override
                fun bind(dataContext: DataContext?): Enumerable<Array<Object>> {
                    val enumerable: Enumerable<*> = bindable.bind(dataContext)
                    return object : AbstractEnumerable<Array<Object?>?>() {
                        @Override
                        fun enumerator(): Enumerator<Array<Object>> {
                            val enumerator: Enumerator<*> = enumerable.enumerator()
                            return object : Enumerator<Array<Object?>?>() {
                                @Override
                                @Nullable
                                fun current(): Array<Object> {
                                    return arrayOf<Object>(enumerator.current())
                                }

                                @Override
                                fun moveNext(): Boolean {
                                    return enumerator.moveNext()
                                }

                                @Override
                                fun reset() {
                                    enumerator.reset()
                                }

                                @Override
                                fun close() {
                                    enumerator.close()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
