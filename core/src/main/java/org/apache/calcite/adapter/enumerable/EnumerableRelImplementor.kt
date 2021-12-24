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
 * Subclass of [org.apache.calcite.plan.RelImplementor] for relational
 * operators of [EnumerableConvention] calling convention.
 */
class EnumerableRelImplementor(
    rexBuilder: RexBuilder?,
    internalParameters: Map<String?, Object?>
) : JavaRelImplementor(rexBuilder) {
    val map: Map<String?, Object?>
    private val corrVars: Map<String, RexToLixTranslator.InputGetter> = HashMap()
    private val stashedParameters: IdentityHashMap<Object, ParameterExpression> = IdentityHashMap()

    @SuppressWarnings("methodref.receiver.bound.invalid")
    val allCorrelateVariables: Function1<String, RexToLixTranslator.InputGetter> =
        Function1<String, RexToLixTranslator.InputGetter> { name: String -> getCorrelVariableGetter(name) }

    init {
        map = internalParameters
    }

    fun visitChild(
        parent: EnumerableRel?,
        ordinal: Int,
        child: EnumerableRel,
        prefer: EnumerableRel.Prefer?
    ): EnumerableRel.Result {
        if (parent != null) {
            assert(child === parent.getInputs().get(ordinal))
        }
        return child.implement(this, prefer)
    }

    fun implementRoot(
        rootRel: EnumerableRel,
        prefer: EnumerableRel.Prefer?
    ): ClassDeclaration {
        var result: EnumerableRel.Result
        result = try {
            rootRel.implement(this, prefer)
        } catch (e: RuntimeException) {
            val ex = IllegalStateException(
                "Unable to implement "
                        + RelOptUtil.toString(rootRel, SqlExplainLevel.ALL_ATTRIBUTES)
            )
            ex.addSuppressed(e)
            throw ex
        }
        when (prefer) {
            ARRAY -> if (result.physType.getFormat() === JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() === 1
            ) {
                val bb = BlockBuilder()
                var e: Expression? = null
                for (statement in result.block.statements) {
                    if (statement is GotoStatement) {
                        e = bb.append(
                            "v",
                            requireNonNull((statement as GotoStatement).expression, "expression")
                        )
                    } else {
                        bb.add(statement)
                    }
                }
                if (e != null) {
                    bb.add(
                        Expressions.return_(
                            null,
                            Expressions.call(null, BuiltInMethod.SLICE0.method, e)
                        )
                    )
                }
                result = Result(
                    bb.toBlock(), result.physType,
                    JavaRowFormat.SCALAR
                )
            }
            else -> {}
        }
        val memberDeclarations: List<MemberDeclaration> = ArrayList()
        TypeRegistrar(memberDeclarations).go(result)

        // This creates the following code
        // final Integer v1stashed = (Integer) root.get("v1stashed")
        // It is convenient for passing non-literal "compile-time" constants
        val stashed: Collection<Statement> = Collections2.transform(
            stashedParameters.values()
        ) { input ->
            Expressions.declare(
                Modifier.FINAL, input,
                Expressions.convert_(
                    Expressions.call(
                        DataContext.ROOT,
                        BuiltInMethod.DATA_CONTEXT_GET.method,
                        Expressions.constant(input.name)
                    ),
                    input.type
                )
            )
        }
        val block: BlockStatement = Expressions.block(
            Iterables.concat(
                stashed,
                result.block.statements
            )
        )
        memberDeclarations.add(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                Enumerable::class.java,
                BuiltInMethod.BINDABLE_BIND.method.getName(),
                Expressions.list(DataContext.ROOT),
                block
            )
        )
        memberDeclarations.add(
            Expressions.methodDecl(
                Modifier.PUBLIC, Class::class.java,
                BuiltInMethod.TYPED_GET_ELEMENT_TYPE.method.getName(),
                ImmutableList.of(),
                Blocks.toFunctionBlock(
                    Expressions.return_(
                        null,
                        Expressions.constant(result.physType.getJavaRowType())
                    )
                )
            )
        )
        return Expressions.classDecl(
            Modifier.PUBLIC,
            "Baz",
            null,
            Collections.singletonList(Bindable::class.java),
            memberDeclarations
        )
    }

    /**
     * Stashes a value for the executor. Given values are de-duplicated if
     * identical (see [java.util.IdentityHashMap]).
     *
     *
     * For instance, to pass `ArrayList` to your method, you can use
     * `Expressions.call(method, implementor.stash(arrayList))`.
     *
     *
     * For simple literals (strings, numbers) the result is equivalent to
     * [org.apache.calcite.linq4j.tree.Expressions.constant].
     *
     *
     * Note: the input value is held in memory as long as the statement
     * is alive. If you are using just a subset of its content, consider creating
     * a slimmer holder.
     *
     * @param input Value to be stashed
     * @param clazz Java class type of the value when it is used
     * @param <T> Java class type of the value when it is used
     * @return Expression that will represent `input` in runtime
    </T> */
    fun <T> stash(input: T?, clazz: Class<in T>?): Expression? {
        // Well-known final classes that can be used as literals
        if (input == null || input is String
            || input is Boolean
            || input is Byte
            || input is Short
            || input is Integer
            || input is Long
            || input is Float
            || input is Double
        ) {
            return Expressions.constant(input, clazz)
        }
        val cached: ParameterExpression = stashedParameters.get(input)
        if (cached != null) {
            return cached
        }
        // "stashed" avoids name clash since this name will be used as the variable
        // name at the very start of the method.
        val name = "v" + map.size().toString() + "stashed"
        val x: ParameterExpression = Expressions.variable(clazz, name)
        map.put(name, input)
        stashedParameters.put(input, x)
        return x
    }

    fun registerCorrelVariable(
        name: String,
        pe: ParameterExpression?,
        corrBlock: BlockBuilder, physType: PhysType
    ) {
        corrVars.put(name) { list, index, storageType ->
            val fieldReference: Expression = physType.fieldReference(pe, index, storageType)
            corrBlock.append(name + "_" + index, fieldReference)
        }
    }

    fun clearCorrelVariable(name: String) {
        assert(corrVars.containsKey(name)) {
            ("Correlation variable " + name
                    + " should be defined")
        }
        corrVars.remove(name)
    }

    fun getCorrelVariableGetter(name: String): RexToLixTranslator.InputGetter? {
        assert(corrVars.containsKey(name)) {
            ("Correlation variable " + name
                    + " should be defined")
        }
        return corrVars[name]
    }

    fun result(physType: PhysType, block: BlockStatement?): EnumerableRel.Result {
        return Result(
            block, physType, (physType as PhysTypeImpl).format
        )
    }

    @get:Override
    val conformance: SqlConformance
        get() = map.getOrDefault(
            "_conformance",
            SqlConformanceEnum.DEFAULT
        ) as SqlConformance

    /** Visitor that finds types in an [Expression] tree.  */
    @VisibleForTesting
    internal class TypeFinder(types: Collection<Type>) : VisitorImpl<Void?>() {
        private val types: Collection<Type>

        init {
            this.types = types
        }

        @Override
        fun visit(newExpression: NewExpression): Void {
            types.add(newExpression.type)
            return super.visit(newExpression)
        }

        @Override
        fun visit(newArrayExpression: NewArrayExpression): Void {
            var type: Type? = newArrayExpression.type
            while (true) {
                val componentType: Type = Types.getComponentType(type) ?: break
                type = componentType
            }
            types.add(type)
            return super.visit(newArrayExpression)
        }

        @Override
        fun visit(constantExpression: ConstantExpression): Void {
            val value: Object = constantExpression.value
            if (value is Type) {
                types.add(value as Type)
            }
            if (value == null) {
                // null literal
                val type: Type = constantExpression.getType()
                types.add(type)
            }
            return super.visit(constantExpression)
        }

        @Override
        fun visit(functionExpression: FunctionExpression): Void {
            val list: List<ParameterExpression> = functionExpression.parameterList
            for (pe in list) {
                types.add(pe.getType())
            }
            if (functionExpression.body == null) {
                return super.visit(functionExpression)
            }
            types.add(functionExpression.body.getType())
            return super.visit(functionExpression)
        }

        @Override
        fun visit(unaryExpression: UnaryExpression): Void {
            if (unaryExpression.nodeType === ExpressionType.Convert) {
                types.add(unaryExpression.getType())
            }
            return super.visit(unaryExpression)
        }
    }

    /** Adds a declaration of each synthetic type found in a code block.  */
    private class TypeRegistrar internal constructor(memberDeclarations: List<MemberDeclaration>) {
        private val memberDeclarations: List<MemberDeclaration>
        private val seen: Set<Type> = HashSet()

        init {
            this.memberDeclarations = memberDeclarations
        }

        private fun register(type: Type) {
            if (!seen.add(type)) {
                return
            }
            if (type is JavaTypeFactoryImpl.SyntheticRecordType) {
                memberDeclarations.add(
                    classDecl(type as JavaTypeFactoryImpl.SyntheticRecordType)
                )
            }
            if (type is ParameterizedType) {
                for (type1 in (type as ParameterizedType).getActualTypeArguments()) {
                    register(type1)
                }
            }
        }

        fun go(result: EnumerableRel.Result) {
            val types: Set<Type> = LinkedHashSet()
            result.block.accept(TypeFinder(types))
            types.add(result.physType.getJavaRowType())
            for (type in types) {
                register(type)
            }
        }
    }

    companion object {
        private fun classDecl(
            type: JavaTypeFactoryImpl.SyntheticRecordType
        ): ClassDeclaration {
            val classDeclaration: ClassDeclaration = Expressions.classDecl(
                Modifier.PUBLIC or Modifier.STATIC,
                type.getName(),
                null,
                ImmutableList.of(Serializable::class.java),
                ArrayList()
            )

            // For each field:
            //   public T0 f0;
            //   ...
            for (field in type.getRecordFields()) {
                classDeclaration.memberDeclarations.add(
                    Expressions.fieldDecl(
                        field.getModifiers(),
                        Expressions.parameter(
                            field.getType(), field.getName()
                        ),
                        null
                    )
                )
            }

            // Constructor:
            //   Foo(T0 f0, ...) { this.f0 = f0; ... }
            val blockBuilder = BlockBuilder()
            val parameters: List<ParameterExpression> = ArrayList()
            val thisParameter: ParameterExpression = Expressions.parameter(type, "this")

            // Here a constructor without parameter is used because the generated
            // code could cause error if number of fields is too large.
            classDeclaration.memberDeclarations.add(
                Expressions.constructorDecl(
                    Modifier.PUBLIC,
                    type,
                    parameters,
                    blockBuilder.toBlock()
                )
            )

            // equals method():
            //   public boolean equals(Object o) {
            //       if (this == o) return true;
            //       if (!(o instanceof MyClass)) return false;
            //       final MyClass that = (MyClass) o;
            //       return this.f0 == that.f0
            //         && equal(this.f1, that.f1)
            //         ...
            //   }
            val blockBuilder2 = BlockBuilder()
            val thatParameter: ParameterExpression = Expressions.parameter(type, "that")
            val oParameter: ParameterExpression = Expressions.parameter(Object::class.java, "o")
            blockBuilder2.add(
                Expressions.ifThen(
                    Expressions.equal(thisParameter, oParameter),
                    Expressions.return_(null, Expressions.constant(true))
                )
            )
            blockBuilder2.add(
                Expressions.ifThen(
                    Expressions.not(
                        Expressions.typeIs(oParameter, type)
                    ),
                    Expressions.return_(null, Expressions.constant(false))
                )
            )
            blockBuilder2.add(
                Expressions.declare(
                    Modifier.FINAL,
                    thatParameter,
                    Expressions.convert_(oParameter, type)
                )
            )
            val conditions: List<Expression> = ArrayList()
            for (field in type.getRecordFields()) {
                conditions.add(
                    if (Primitive.`is`(field.getType())) Expressions.equal(
                        Expressions.field(thisParameter, field.getName()),
                        Expressions.field(thatParameter, field.getName())
                    ) else Expressions.call(
                        BuiltInMethod.OBJECTS_EQUAL.method,
                        Expressions.field(thisParameter, field.getName()),
                        Expressions.field(thatParameter, field.getName())
                    )
                )
            }
            blockBuilder2.add(
                Expressions.return_(null, Expressions.foldAnd(conditions))
            )
            classDeclaration.memberDeclarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    Boolean::class.javaPrimitiveType,
                    "equals",
                    Collections.singletonList(oParameter),
                    blockBuilder2.toBlock()
                )
            )

            // hashCode method:
            //   public int hashCode() {
            //     int h = 0;
            //     h = hash(h, f0);
            //     ...
            //     return h;
            //   }
            val blockBuilder3 = BlockBuilder()
            val hParameter: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, "h")
            val constantZero: ConstantExpression = Expressions.constant(0)
            blockBuilder3.add(
                Expressions.declare(0, hParameter, constantZero)
            )
            for (field in type.getRecordFields()) {
                val method: Method = BuiltInMethod.HASH.method
                blockBuilder3.add(
                    Expressions.statement(
                        Expressions.assign(
                            hParameter,
                            Expressions.call(
                                method.getDeclaringClass(),
                                method.getName(),
                                ImmutableList.of(
                                    hParameter,
                                    Expressions.field(thisParameter, field)
                                )
                            )
                        )
                    )
                )
            }
            blockBuilder3.add(
                Expressions.return_(null, hParameter)
            )
            classDeclaration.memberDeclarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    Int::class.javaPrimitiveType,
                    "hashCode",
                    Collections.emptyList(),
                    blockBuilder3.toBlock()
                )
            )

            // compareTo method:
            //   public int compareTo(MyClass that) {
            //     int c;
            //     c = compare(this.f0, that.f0);
            //     if (c != 0) return c;
            //     ...
            //     return 0;
            //   }
            val blockBuilder4 = BlockBuilder()
            val cParameter: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, "c")
            val mod = if (type.getRecordFields().size() === 1) Modifier.FINAL else 0
            blockBuilder4.add(
                Expressions.declare(mod, cParameter, null)
            )
            val conditionalStatement: ConditionalStatement = Expressions.ifThen(
                Expressions.notEqual(cParameter, constantZero),
                Expressions.return_(null, cParameter)
            )
            for (field in type.getRecordFields()) {
                var compareCall: MethodCallExpression
                compareCall = try {
                    val method: Method =
                        (if (field.nullable()) BuiltInMethod.COMPARE_NULLS_LAST else BuiltInMethod.COMPARE).method
                    Expressions.call(
                        method.getDeclaringClass(),
                        method.getName(),
                        Expressions.field(thisParameter, field),
                        Expressions.field(thatParameter, field)
                    )
                } catch (e: RuntimeException) {
                    if (e.getCause() is NoSuchMethodException) {
                        // Just ignore the field in compareTo
                        // "create synthetic record class" blindly creates compareTo for
                        // all the fields, however not all the records will actually be used
                        // as sorting keys (e.g. temporary state for aggregate calculation).
                        // In those cases it is fine if we skip the problematic fields.
                        continue
                    }
                    throw e
                }
                blockBuilder4.add(
                    Expressions.statement(
                        Expressions.assign(
                            cParameter,
                            compareCall
                        )
                    )
                )
                blockBuilder4.add(conditionalStatement)
            }
            blockBuilder4.add(
                Expressions.return_(null, constantZero)
            )
            classDeclaration.memberDeclarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    Int::class.javaPrimitiveType,
                    "compareTo",
                    Collections.singletonList(thatParameter),
                    blockBuilder4.toBlock()
                )
            )

            // toString method:
            //   public String toString() {
            //     return "{f0=" + f0
            //       + ", f1=" + f1
            //       ...
            //       + "}";
            //   }
            val blockBuilder5 = BlockBuilder()
            var expression5: Expression? = null
            for (field in type.getRecordFields()) {
                expression5 = if (expression5 == null) {
                    Expressions.constant("{" + field.getName().toString() + "=")
                } else {
                    Expressions.add(
                        expression5,
                        Expressions.constant(", " + field.getName().toString() + "=")
                    )
                }
                expression5 = Expressions.add(
                    expression5,
                    Expressions.field(thisParameter, field.getName())
                )
            }
            expression5 = if (expression5 == null) Expressions.constant("{}") else Expressions.add(
                expression5,
                Expressions.constant("}")
            )
            blockBuilder5.add(
                Expressions.return_(
                    null,
                    expression5
                )
            )
            classDeclaration.memberDeclarations.add(
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    String::class.java,
                    "toString",
                    Collections.emptyList(),
                    blockBuilder5.toBlock()
                )
            )
            return classDeclaration
        }
    }
}
