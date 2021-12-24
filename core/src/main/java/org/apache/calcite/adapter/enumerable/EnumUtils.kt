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

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Utilities for generating programs in the Enumerable (functional)
 * style.
 */
object EnumUtils {
    const val BRIDGE_METHODS = true
    val NO_PARAMS: List<ParameterExpression> = ImmutableList.of()
    val NO_EXPRS: List<Expression> = ImmutableList.of()
    val LEFT_RIGHT: List<String> = ImmutableList.of("left", "right")

    /** Declares a method that overrides another method.  */
    fun overridingMethodDecl(
        method: Method,
        parameters: Iterable<ParameterExpression?>?,
        body: BlockStatement?
    ): MethodDeclaration {
        return Expressions.methodDecl(
            method.getModifiers() and Modifier.ABSTRACT.inv(),
            method.getReturnType(),
            method.getName(),
            parameters,
            body
        )
    }

    fun javaClass(
        typeFactory: JavaTypeFactory, type: RelDataType?
    ): Type {
        val clazz: Type = typeFactory.getJavaClass(type)
        return if (clazz is Class) clazz else Array<Object>::class.java
    }

    fun fieldTypes(
        typeFactory: JavaTypeFactory,
        inputTypes: List<RelDataType?>
    ): List<Type> {
        return object : AbstractList<Type?>() {
            @Override
            operator fun get(index: Int): Type {
                return javaClass(typeFactory, inputTypes[index])
            }

            @Override
            fun size(): Int {
                return inputTypes.size()
            }
        }
    }

    fun fieldRowTypes(
        inputRowType: RelDataType,
        @Nullable extraInputs: List<RexNode?>?,
        argList: List<Integer>
    ): List<RelDataType> {
        val inputFields: List<RelDataTypeField> = inputRowType.getFieldList()
        return object : AbstractList<RelDataType?>() {
            @Override
            operator fun get(index: Int): RelDataType {
                val arg: Int = argList[index]
                return if (arg < inputFields.size()) inputFields[arg].getType() else requireNonNull(
                    extraInputs,
                    "extraInputs"
                )
                    .get(arg - inputFields.size()).getType()
            }

            @Override
            fun size(): Int {
                return argList.size()
            }
        }
    }

    fun joinSelector(
        joinType: JoinRelType, physType: PhysType,
        inputPhysTypes: List<PhysType?>?
    ): Expression {
        // A parameter for each input.
        val parameters: List<ParameterExpression> = ArrayList()

        // Generate all fields.
        val expressions: List<Expression> = ArrayList()
        val outputFieldCount: Int = physType.getRowType().getFieldCount()
        for (ord in Ord.zip(inputPhysTypes)) {
            val inputPhysType: PhysType = ord.e.makeNullable(joinType.generatesNullsOn(ord.i))
            // If input item is just a primitive, we do not generate specialized
            // primitive apply override since it won't be called anyway
            // Function<T> always operates on boxed arguments
            val parameter: ParameterExpression = Expressions.parameter(
                Primitive.box(inputPhysType.getJavaRowType()),
                LEFT_RIGHT[ord.i]
            )
            parameters.add(parameter)
            if (expressions.size() === outputFieldCount) {
                // For instance, if semi-join needs to return just the left inputs
                break
            }
            val fieldCount: Int = inputPhysType.getRowType().getFieldCount()
            for (i in 0 until fieldCount) {
                var expression: Expression = inputPhysType.fieldReference(
                    parameter, i,
                    physType.getJavaFieldType(expressions.size())
                )
                if (joinType.generatesNullsOn(ord.i)) {
                    expression = Expressions.condition(
                        Expressions.equal(parameter, Expressions.constant(null)),
                        Expressions.constant(null),
                        expression
                    )
                }
                expressions.add(expression)
            }
        }
        return Expressions.lambda(
            Function2::class.java,
            physType.record(expressions),
            parameters
        )
    }

    /**
     * In Calcite, `java.sql.Date` and `java.sql.Time` are
     * stored as `Integer` type, `java.sql.Timestamp` is
     * stored as `Long` type.
     */
    fun toInternal(operand: Expression, @Nullable targetType: Type?): Expression? {
        return toInternal(operand, operand.getType(), targetType)
    }

    private fun toInternal(
        operand: Expression?,
        fromType: Type?, @Nullable targetType: Type?
    ): Expression? {
        if (fromType === java.sql.Date::class.java) {
            if (targetType === Int::class.javaPrimitiveType) {
                return Expressions.call(BuiltInMethod.DATE_TO_INT.method, operand)
            } else if (targetType === Integer::class.java) {
                return Expressions.call(BuiltInMethod.DATE_TO_INT_OPTIONAL.method, operand)
            }
        } else if (fromType === Time::class.java) {
            if (targetType === Int::class.javaPrimitiveType) {
                return Expressions.call(BuiltInMethod.TIME_TO_INT.method, operand)
            } else if (targetType === Integer::class.java) {
                return Expressions.call(BuiltInMethod.TIME_TO_INT_OPTIONAL.method, operand)
            }
        } else if (fromType === java.sql.Timestamp::class.java) {
            if (targetType === Long::class.javaPrimitiveType) {
                return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG.method, operand)
            } else if (targetType === Long::class.java) {
                return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method, operand)
            }
        }
        return operand
    }

    /** Converts from internal representation to JDBC representation used by
     * arguments of user-defined functions. For example, converts date values from
     * `int` to [java.sql.Date].  */
    private fun fromInternal(operand: Expression, targetType: Type): Expression? {
        return fromInternal(operand, operand.getType(), targetType)
    }

    private fun fromInternal(
        operand: Expression?,
        fromType: Type?, targetType: Type?
    ): Expression? {
        if (operand === ConstantUntypedNull.INSTANCE) {
            return operand
        }
        if (operand.getType() !is Class) {
            return operand
        }
        if (Types.isAssignableFrom(targetType, fromType)) {
            return operand
        }
        if (targetType === java.sql.Date::class.java) {
            // E.g. from "int" or "Integer" to "java.sql.Date",
            // generate "SqlFunctions.internalToDate".
            if (isA(fromType, Primitive.INT)) {
                return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, operand)
            }
        } else if (targetType === Time::class.java) {
            // E.g. from "int" or "Integer" to "java.sql.Time",
            // generate "SqlFunctions.internalToTime".
            if (isA(fromType, Primitive.INT)) {
                return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, operand)
            }
        } else if (targetType === java.sql.Timestamp::class.java) {
            // E.g. from "long" or "Long" to "java.sql.Timestamp",
            // generate "SqlFunctions.internalToTimestamp".
            if (isA(fromType, Primitive.LONG)) {
                return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, operand)
            }
        }
        return if (Primitive.`is`(operand.type)
            && Primitive.isBox(targetType)
        ) {
            // E.g. operand is "int", target is "Long", generate "(long) operand".
            Expressions.convert_(
                operand,
                Primitive.unbox(targetType)
            )
        } else operand
    }

    fun fromInternal(
        targetTypes: Array<Class<*>>,
        expressions: List<Expression?>
    ): List<Expression> {
        val list: List<Expression> = ArrayList()
        if (targetTypes.size == expressions.size()) {
            for (i in 0 until expressions.size()) {
                list.add(fromInternal(expressions[i], targetTypes[i]))
            }
        } else {
            var j = 0
            for (i in 0 until expressions.size()) {
                var type: Class<*>
                if (!targetTypes[j].isArray()) {
                    type = targetTypes[j]
                    j++
                } else {
                    type = targetTypes[j].getComponentType()
                }
                list.add(fromInternal(expressions[i], type))
            }
        }
        return list
    }

    fun fromInternal(type: Type): Type {
        if (type === java.sql.Date::class.java || type === Time::class.java) {
            return Int::class.javaPrimitiveType
        }
        return if (type === java.sql.Timestamp::class.java) {
            Long::class.javaPrimitiveType
        } else type
    }

    @Nullable
    private fun toInternal(type: RelDataType): Type {
        return toInternal(type, false)
    }

    @Nullable
    fun toInternal(type: RelDataType, forceNotNull: Boolean): Type? {
        return when (type.getSqlTypeName()) {
            DATE, TIME -> if (type.isNullable() && !forceNotNull) Integer::class.java else Int::class.javaPrimitiveType
            TIMESTAMP -> if (type.isNullable() && !forceNotNull) Long::class.java else Long::class.javaPrimitiveType
            else -> null // we don't care; use the default storage type
        }
    }

    fun internalTypes(operandList: List<RexNode?>?): List<Type> {
        return Util.transform(operandList) { node -> toInternal(node.getType()) }
    }

    /**
     * Convert `operand` to target type `toType`.
     *
     * @param operand The expression to convert
     * @param toType  Target type
     * @return A new expression with type `toType` or original if there
     * is no need to convert
     */
    fun convert(operand: Expression?, toType: Type?): Expression? {
        val fromType: Type = operand.getType()
        return convert(operand, fromType, toType)
    }

    /**
     * Convert `operand` to target type `toType`.
     *
     * @param operand  The expression to convert
     * @param fromType Field type
     * @param toType   Target type
     * @return A new expression with type `toType` or original if there
     * is no need to convert
     */
    fun convert(
        operand: Expression?, fromType: Type?,
        toType: Type?
    ): Expression? {
        if (!Types.needTypeCast(fromType, toType)) {
            return operand
        }
        // E.g. from "Short" to "int".
        // Generate "x.intValue()".
        val toPrimitive: Primitive = Primitive.of(toType)
        val toBox: Primitive = Primitive.ofBox(toType)
        val fromBox: Primitive = Primitive.ofBox(fromType)
        val fromPrimitive: Primitive = Primitive.of(fromType)
        val fromNumber = (fromType is Class
                && Number::class.java.isAssignableFrom(fromType as Class?))
        if (fromType === String::class.java) {
            if (toPrimitive != null) {
                return when (toPrimitive) {
                    CHAR, SHORT, INT, LONG, FLOAT, DOUBLE ->           // Generate "SqlFunctions.toShort(x)".
                        Expressions.call(
                            SqlFunctions::class.java,
                            "to" + SqlFunctions.initcap(toPrimitive.getPrimitiveName()),
                            operand
                        )
                    else ->           // Generate "Short.parseShort(x)".
                        Expressions.call(
                            toPrimitive.getBoxClass(),
                            "parse" + SqlFunctions.initcap(toPrimitive.getPrimitiveName()),
                            operand
                        )
                }
            }
            if (toBox != null) {
                return when (toBox) {
                    CHAR ->           // Generate "SqlFunctions.toCharBoxed(x)".
                        Expressions.call(
                            SqlFunctions::class.java,
                            "to" + SqlFunctions.initcap(toBox.getPrimitiveName()).toString() + "Boxed",
                            operand
                        )
                    else ->           // Generate "Short.valueOf(x)".
                        Expressions.call(
                            toBox.getBoxClass(),
                            "valueOf",
                            operand
                        )
                }
            }
        }
        if (toPrimitive != null) {
            if (fromPrimitive != null) {
                // E.g. from "float" to "double"
                return Expressions.convert_(
                    operand, toPrimitive.getPrimitiveClass()
                )
            }
            return if (fromNumber || fromBox === Primitive.CHAR) {
                // Generate "x.shortValue()".
                Expressions.unbox(operand, toPrimitive)
            } else {
                // E.g. from "Object" to "short".
                // Generate "SqlFunctions.toShort(x)"
                Expressions.call(
                    SqlFunctions::class.java,
                    "to" + SqlFunctions.initcap(toPrimitive.getPrimitiveName()),
                    operand
                )
            }
        } else if (fromNumber && toBox != null) {
            // E.g. from "Short" to "Integer"
            // Generate "x == null ? null : Integer.valueOf(x.intValue())"
            return Expressions.condition(
                Expressions.equal(operand, RexImpTable.NULL_EXPR),
                RexImpTable.NULL_EXPR,
                Expressions.box(
                    Expressions.unbox(operand, toBox),
                    toBox
                )
            )
        } else if (fromPrimitive != null && toBox != null) {
            // E.g. from "int" to "Long".
            // Generate Long.valueOf(x)
            // Eliminate primitive casts like Long.valueOf((long) x)
            if (operand is UnaryExpression) {
                val una: UnaryExpression? = operand as UnaryExpression?
                if (una.nodeType === ExpressionType.Convert
                    && Primitive.of(una.getType()) === toBox
                ) {
                    val origin: Primitive = Primitive.of(una.expression.type)
                    if (origin != null && toBox.assignableFrom(origin)) {
                        return Expressions.box(una.expression, toBox)
                    }
                }
            }
            return if (fromType === toBox.primitiveClass) {
                Expressions.box(operand, toBox)
            } else Expressions.box(
                Expressions.convert_(operand, toBox.getPrimitiveClass()),
                toBox
            )
            // E.g., from "int" to "Byte".
            // Convert it first and generate "Byte.valueOf((byte)x)"
            // Because there is no method "Byte.valueOf(int)" in Byte
        }
        // Convert datetime types to internal storage type:
        // 1. java.sql.Date -> int or Integer
        // 2. java.sql.Time -> int or Integer
        // 3. java.sql.Timestamp -> long or Long
        if (representAsInternalType(fromType)) {
            val internalTypedOperand: Expression? = toInternal(operand, fromType, toType)
            if (operand !== internalTypedOperand) {
                return internalTypedOperand
            }
        }
        // Convert internal storage type to datetime types:
        // 1. int or Integer -> java.sql.Date
        // 2. int or Integer -> java.sql.Time
        // 3. long or Long -> java.sql.Timestamp
        if (representAsInternalType(toType)) {
            val originTypedOperand: Expression? = fromInternal(operand, fromType, toType)
            if (operand !== originTypedOperand) {
                return originTypedOperand
            }
        }
        if (toType === BigDecimal::class.java) {
            if (fromBox != null) {
                // E.g. from "Integer" to "BigDecimal".
                // Generate "x == null ? null : new BigDecimal(x.intValue())"
                return Expressions.condition(
                    Expressions.equal(operand, RexImpTable.NULL_EXPR),
                    RexImpTable.NULL_EXPR,
                    Expressions.new_(
                        BigDecimal::class.java,
                        Expressions.unbox(operand, fromBox)
                    )
                )
            }
            return if (fromPrimitive != null) {
                // E.g. from "int" to "BigDecimal".
                // Generate "new BigDecimal(x)"
                Expressions.new_(BigDecimal::class.java, operand)
            } else Expressions.condition(
                Expressions.equal(operand, RexImpTable.NULL_EXPR),
                RexImpTable.NULL_EXPR,
                Expressions.call(
                    SqlFunctions::class.java,
                    "toBigDecimal",
                    operand
                )
            )
            // E.g. from "Object" to "BigDecimal".
            // Generate "x == null ? null : SqlFunctions.toBigDecimal(x)"
        } else if (toType === String::class.java) {
            return if (fromPrimitive != null) {
                when (fromPrimitive) {
                    DOUBLE, FLOAT ->           // E.g. from "double" to "String"
                        // Generate "SqlFunctions.toString(x)"
                        Expressions.call(
                            SqlFunctions::class.java,
                            "toString",
                            operand
                        )
                    else ->           // E.g. from "int" to "String"
                        // Generate "Integer.toString(x)"
                        Expressions.call(
                            fromPrimitive.getBoxClass(),
                            "toString",
                            operand
                        )
                }
            } else if (fromType === BigDecimal::class.java) {
                // E.g. from "BigDecimal" to "String"
                // Generate "SqlFunctions.toString(x)"
                Expressions.condition(
                    Expressions.equal(operand, RexImpTable.NULL_EXPR),
                    RexImpTable.NULL_EXPR,
                    Expressions.call(
                        SqlFunctions::class.java,
                        "toString",
                        operand
                    )
                )
            } else {
                val result: Expression
                result = try {
                    // Avoid to generate code like:
                    // "null.toString()" or "(xxx) null.toString()"
                    if (operand is ConstantExpression) {
                        val ce: ConstantExpression? = operand as ConstantExpression?
                        if (ce.value == null) {
                            return Expressions.convert_(operand, toType)
                        }
                    }
                    // Try to call "toString()" method
                    // E.g. from "Integer" to "String"
                    // Generate "x == null ? null : x.toString()"
                    Expressions.condition(
                        Expressions.equal(operand, RexImpTable.NULL_EXPR),
                        RexImpTable.NULL_EXPR,
                        Expressions.call(operand, "toString")
                    )
                } catch (e: RuntimeException) {
                    // For some special cases, e.g., "BuiltInMethod.LESSER",
                    // its return type is generic ("Comparable"), which contains
                    // no "toString()" method. We fall through to "(String)x".
                    return Expressions.convert_(operand, toType)
                }
                result
            }
        }
        return Expressions.convert_(operand, toType)
    }

    /** Converts a value to a given class.  */
    fun <T> evaluate(o: Object?, clazz: Class<T>): @Nullable T? {
        // We need optimization here for constant folding.
        // Not all the expressions can be interpreted (e.g. ternary), so
        // we rely on optimization capabilities to fold non-interpretable
        // expressions.
        var clazz: Class<T> = clazz
        clazz = Primitive.box(clazz)
        val bb = BlockBuilder()
        val expr: Expression? = convert(Expressions.constant(o), clazz)
        bb.add(Expressions.return_(null, expr))
        val convert: FunctionExpression<*> = Expressions.lambda(bb.toBlock(), ImmutableList.of())
        return clazz.cast(convert.compile().dynamicInvoke())
    }

    private fun isA(fromType: Type?, primitive: Primitive): Boolean {
        return (Primitive.of(fromType) === primitive
                || Primitive.ofBox(fromType) === primitive)
    }

    private fun representAsInternalType(type: Type?): Boolean {
        return type === java.sql.Date::class.java || type === Time::class.java || type === java.sql.Timestamp::class.java
    }

    /**
     * In [org.apache.calcite.sql.type.SqlTypeAssignmentRule],
     * some rules decide whether one type can be assignable to another type.
     * Based on these rules, a function can accept arguments with assignable types.
     *
     *
     * For example, a function with Long type operand can accept Integer as input.
     * See `org.apache.calcite.sql.SqlUtil#filterRoutinesByParameterType()` for details.
     *
     *
     * During query execution, some of the assignable types need explicit conversion
     * to the target types. i.e., Decimal expression should be converted to Integer
     * before it is assigned to the Integer type Lvalue(In Java, Decimal can not be assigned to
     * Integer directly).
     *
     * @param targetTypes Formal operand types declared for the function arguments
     * @param arguments Input expressions to the function
     * @return Input expressions with probable type conversion
     */
    fun convertAssignableTypes(
        targetTypes: Array<Class<*>>,
        arguments: List<Expression?>
    ): List<Expression> {
        val list: List<Expression> = ArrayList()
        if (targetTypes.size == arguments.size()) {
            for (i in 0 until arguments.size()) {
                list.add(convertAssignableType(arguments[i], targetTypes[i]))
            }
        } else {
            var j = 0
            for (argument in arguments) {
                var type: Class<*>
                if (!targetTypes[j].isArray()) {
                    type = targetTypes[j]
                    j++
                } else {
                    type = targetTypes[j].getComponentType()
                }
                list.add(convertAssignableType(argument, type))
            }
        }
        return list
    }

    /**
     * Handles decimal type specifically with explicit type conversion.
     */
    private fun convertAssignableType(
        argument: Expression?, targetType: Type
    ): Expression? {
        return if (targetType !== BigDecimal::class.java) {
            argument
        } else convert(argument, targetType)
    }

    /**
     * A more powerful version of
     * [org.apache.calcite.linq4j.tree.Expressions.call].
     * Tries best effort to convert the
     * accepted arguments to match parameter type.
     *
     * @param targetExpression Target expression, or null if method is static
     * @param clazz Class against which method is invoked
     * @param methodName Name of method
     * @param arguments Argument expressions
     *
     * @return MethodCallExpression that call the given name method
     * @throws RuntimeException if no suitable method found
     */
    fun call(
        @Nullable targetExpression: Expression?,
        clazz: Class, methodName: String, arguments: List<Expression?>
    ): MethodCallExpression {
        val argumentTypes: Array<Class> = Types.toClassArray(arguments)
        return try {
            val candidate: Method = clazz.getMethod(methodName, argumentTypes)
            Expressions.call(targetExpression, candidate, arguments)
        } catch (e: NoSuchMethodException) {
            for (method in clazz.getMethods()) {
                if (method.getName().equals(methodName)) {
                    val varArgs: Boolean = method.isVarArgs()
                    val parameterTypes: Array<Class<*>> = method.getParameterTypes()
                    if (Types.allAssignable(varArgs, parameterTypes, argumentTypes)) {
                        return Expressions.call(targetExpression, method, arguments)
                    }
                    // fall through
                    val typeMatchedArguments: List<Expression?>? =
                        matchMethodParameterTypes(varArgs, parameterTypes, arguments)
                    if (typeMatchedArguments != null) {
                        return Expressions.call(targetExpression, method, typeMatchedArguments)
                    }
                }
            }
            throw RuntimeException(
                "while resolving method '" + methodName
                        + Arrays.toString(argumentTypes).toString() + "' in class " + clazz, e
            )
        }
    }

    @Nullable
    private fun matchMethodParameterTypes(
        varArgs: Boolean,
        parameterTypes: Array<Class<*>>, arguments: List<Expression?>
    ): List<Expression?>? {
        if (varArgs && arguments.size() < parameterTypes.size - 1
            || !varArgs && arguments.size() !== parameterTypes.size
        ) {
            return null
        }
        val typeMatchedArguments: List<Expression> = ArrayList()
        for (i in 0 until arguments.size()) {
            val parameterType: Class<*> =
                if (!varArgs || i < parameterTypes.size - 1) parameterTypes[i] else Object::class.java
            val typeMatchedArgument: Expression = matchMethodParameterType(
                arguments[i], parameterType
            )
                ?: return null
            typeMatchedArguments.add(typeMatchedArgument)
        }
        return typeMatchedArguments
    }

    /**
     * Matches an argument expression to method parameter type with best effort.
     *
     * @param argument Argument Expression
     * @param parameter Parameter type
     * @return Converted argument expression that matches the parameter type.
     * Returns null if it is impossible to match.
     */
    @Nullable
    private fun matchMethodParameterType(
        argument: Expression?, parameter: Class<*>
    ): Expression? {
        val argumentType: Type = argument.getType()
        if (Types.isAssignableFrom(parameter, argumentType)) {
            return argument
        }
        // Object.class is not assignable from primitive types,
        // but the method with Object parameters can accept primitive types.
        // E.g., "array(Object... args)" in SqlFunctions
        if (parameter === Object::class.java
            && Primitive.of(argumentType) != null
        ) {
            return argument
        }
        // Convert argument with Object.class type to parameter explicitly
        if (argumentType === Object::class.java
            && Primitive.of(argumentType) == null
        ) {
            return convert(argument, parameter)
        }
        // assignable types that can be accepted with explicit conversion
        return if (parameter === BigDecimal::class.java
            && Primitive.ofBoxOr(argumentType) != null
        ) {
            convert(argument, parameter)
        } else null
    }

    /** Transforms a JoinRelType to Linq4j JoinType.  */
    fun toLinq4jJoinType(joinRelType: JoinRelType): JoinType {
        when (joinRelType) {
            INNER -> return JoinType.INNER
            LEFT -> return JoinType.LEFT
            RIGHT -> return JoinType.RIGHT
            FULL -> return JoinType.FULL
            SEMI -> return JoinType.SEMI
            ANTI -> return JoinType.ANTI
            else -> {}
        }
        throw IllegalStateException(
            "Unable to convert $joinRelType to Linq4j JoinType"
        )
    }

    /** Returns a predicate expression based on a join condition.  */
    fun generatePredicate(
        implementor: EnumerableRelImplementor,
        rexBuilder: RexBuilder?,
        left: RelNode,
        right: RelNode,
        leftPhysType: PhysType,
        rightPhysType: PhysType,
        condition: RexNode?
    ): Expression {
        val builder = BlockBuilder()
        val left_: ParameterExpression = Expressions.parameter(leftPhysType.getJavaRowType(), "left")
        val right_: ParameterExpression = Expressions.parameter(rightPhysType.getJavaRowType(), "right")
        val program = RexProgramBuilder(
            implementor.getTypeFactory().builder()
                .addAll(left.getRowType().getFieldList())
                .addAll(right.getRowType().getFieldList())
                .build(),
            rexBuilder
        )
        program.addCondition(condition)
        builder.add(
            Expressions.return_(
                null,
                RexToLixTranslator.translateCondition(
                    program.getProgram(),
                    implementor.getTypeFactory(),
                    builder,
                    InputGetterImpl(
                        ImmutableMap.of(
                            left_, leftPhysType,
                            right_, rightPhysType
                        )
                    ),
                    implementor.allCorrelateVariables,
                    implementor.getConformance()
                )
            )
        )
        return Expressions.lambda(Predicate2::class.java, builder.toBlock(), left_, right_)
    }

    /**
     * Generates a window selector which appends attribute of the window based on
     * the parameters.
     *
     * Note that it only works for batch scenario. E.g. all data is known and there is no late data.
     */
    fun tumblingWindowSelector(
        inputPhysType: PhysType,
        outputPhysType: PhysType,
        wmColExpr: Expression?,
        windowSizeExpr: Expression?,
        offsetExpr: Expression?
    ): Expression {
        // Generate all fields.
        val expressions: List<Expression> = ArrayList()
        // If input item is just a primitive, we do not generate specialized
        // primitive apply override since it won't be called anyway
        // Function<T> always operates on boxed arguments
        val parameter: ParameterExpression =
            Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()), "_input")
        val fieldCount: Int = inputPhysType.getRowType().getFieldCount()
        for (i in 0 until fieldCount) {
            val expression: Expression = inputPhysType.fieldReference(
                parameter, i,
                outputPhysType.getJavaFieldType(expressions.size())
            )
            expressions.add(expression)
        }
        val wmColExprToLong: Expression? = convert(wmColExpr, Long::class.javaPrimitiveType)

        // Find the fixed window for a timestamp given a window size and an offset, and return the
        // window start.
        // wmColExprToLong - (wmColExprToLong + windowSizeMillis - offsetMillis) % windowSizeMillis
        val windowStartExpr: Expression = Expressions.subtract(
            wmColExprToLong,
            Expressions.modulo(
                Expressions.add(
                    wmColExprToLong,
                    Expressions.subtract(
                        windowSizeExpr,
                        offsetExpr
                    )
                ),
                windowSizeExpr
            )
        )
        expressions.add(windowStartExpr)

        // The window end equals to the window start plus window size.
        // windowStartMillis + sizeMillis
        val windowEndExpr: Expression = Expressions.add(
            windowStartExpr,
            windowSizeExpr
        )
        expressions.add(windowEndExpr)
        return Expressions.lambda(
            Function1::class.java,
            outputPhysType.record(expressions),
            parameter
        )
    }

    /**
     * Creates enumerable implementation that applies sessionization to elements from the input
     * enumerator based on a specified key. Elements are windowed into sessions separated by
     * periods with no input for at least the duration specified by gap parameter.
     */
    fun sessionize(
        inputEnumerator: Enumerator<Array<Object?>?>,
        indexOfWatermarkedColumn: Int, indexOfKeyColumn: Int, gap: Long
    ): Enumerable<Array<Object>> {
        return object : AbstractEnumerable<Array<Object?>?>() {
            @Override
            fun enumerator(): Enumerator<Array<Object>> {
                return SessionizationEnumerator(
                    inputEnumerator,
                    indexOfWatermarkedColumn, indexOfKeyColumn, gap
                )
            }
        }
    }

    /**
     * Create enumerable implementation that applies hopping on each element from the input
     * enumerator and produces at least one element for each input element.
     */
    fun hopping(
        inputEnumerator: Enumerator<Array<Object?>?>,
        indexOfWatermarkedColumn: Int, emitFrequency: Long, windowSize: Long, offset: Long
    ): Enumerable<Array<Object>> {
        return object : AbstractEnumerable<Array<Object?>?>() {
            @Override
            fun enumerator(): Enumerator<Array<Object>> {
                return HopEnumerator(
                    inputEnumerator,
                    indexOfWatermarkedColumn, emitFrequency, windowSize, offset
                )
            }
        }
    }

    private fun hopWindows(
        tsMillis: Long, periodMillis: Long, sizeMillis: Long, offsetMillis: Long
    ): List<Pair<Long, Long>> {
        val ret: ArrayList<Pair<Long, Long>> = ArrayList(Math.toIntExact(sizeMillis / periodMillis))
        val lastStart = tsMillis - (tsMillis + periodMillis - offsetMillis) % periodMillis
        var start = lastStart
        while (start > tsMillis - sizeMillis) {
            ret.add(Pair(start, start + sizeMillis))
            start -= periodMillis
        }
        return ret
    }

    /**
     * Apply tumbling per row from the enumerable input.
     */
    fun <TSource, TResult> tumbling(
        inputEnumerable: Enumerable<TSource>,
        outSelector: Function1<TSource, TResult>
    ): Enumerable<TResult> {
        return object : AbstractEnumerable<TResult>() {
            // Applies tumbling on each element from the input enumerator and produces
            // exactly one element for each input element.
            @Override
            fun enumerator(): Enumerator<TResult> {
                return object : Enumerator<TResult>() {
                    val inputs: Enumerator<TSource> = inputEnumerable.enumerator()
                    @Override
                    fun current(): TResult {
                        return outSelector.apply(inputs.current())
                    }

                    @Override
                    fun moveNext(): Boolean {
                        return inputs.moveNext()
                    }

                    @Override
                    fun reset() {
                        inputs.reset()
                    }

                    @Override
                    fun close() {
                        inputs.close()
                    }
                }
            }
        }
    }

    @Nullable
    fun generateCollatorExpression(@Nullable collation: SqlCollation?): Expression? {
        if (collation == null) {
            return null
        }
        val collator: Collator = collation.getCollator() ?: return null

        // Utilities.generateCollator(
        //      new Locale(
        //          collation.getLocale().getLanguage(),
        //          collation.getLocale().getCountry(),
        //          collation.getLocale().getVariant()),
        //      collation.getCollator().getStrength());
        val locale: Locale = collation.getLocale()
        val strength: Int = collator.getStrength()
        return Expressions.call(
            Utilities::class.java,
            "generateCollator",
            Expressions.new_(
                Locale::class.java,
                Expressions.constant(locale.getLanguage()),
                Expressions.constant(locale.getCountry()),
                Expressions.constant(locale.getVariant())
            ),
            Expressions.constant(strength)
        )
    }

    /** Returns a function that converts an internal value to an external
     * value.
     *
     *
     * Datetime values' internal representations have no time zone,
     * and their external values are moments (relative to UTC epoch),
     * so the `timeZone` parameter supplies the implicit time zone of
     * the internal representation. If you specify the local time zone of the
     * JVM, then [Timestamp.toString], [Date.toString], and
     * [Time.toString] on the external values will give a value
     * consistent with the internal values.  */
    fun toExternal(
        type: RelDataType,
        timeZone: TimeZone
    ): Function<Object, Object> {
        return when (type.getSqlTypeName()) {
            DATE -> Function<Object, Object> { o ->
                val d: Int = o as Integer
                var v: Long = d * DateTimeUtils.MILLIS_PER_DAY
                v -= timeZone.getOffset(v)
                Date(v)
            }
            TIME -> Function<Object, Object> { o ->
                var v: Long = o as Integer
                v -= timeZone.getOffset(v)
                Time(v % DateTimeUtils.MILLIS_PER_DAY)
            }
            TIMESTAMP -> Function<Object, Object> { o ->
                var v = o as Long
                v -= timeZone.getOffset(v)
                Timestamp(v)
            }
            else -> Function.identity()
        }
    }

    /** Returns a function that converts an array of internal values to
     * a list of external values.  */
    @SuppressWarnings("unchecked")
    fun toExternal(
        types: List<RelDataType?>, timeZone: TimeZone?
    ): Function<Array<Object>, List<Object>> {
        val functions: Array<Function<Object, Object>?> = arrayOfNulls<Function>(types.size())
        for (i in 0 until types.size()) {
            functions[i] = toExternal(types[i], timeZone)
        }
        @Nullable val objects: Array<Object?> = arrayOfNulls<Object>(types.size())
        return Function<Array<Object>, List<Object>> { values ->
            for (i in 0 until values.length) {
                objects[i] = if (values.get(i) == null) null else functions[i].apply(values.get(i))
            }
            Arrays.asList(objects.clone())
        }
    }

    /** Enumerator that converts rows into sessions separated by gaps.  */
    private class SessionizationEnumerator internal constructor(
        inputEnumerator: Enumerator<Array<Object?>?>,
        indexOfWatermarkedColumn: Int, indexOfKeyColumn: Int, gap: Long
    ) : Enumerator<Array<Object?>?> {
        private val inputEnumerator: Enumerator<Array<Object>>
        private val indexOfWatermarkedColumn: Int
        private val indexOfKeyColumn: Int
        private val gap: Long
        private val list: Deque<Array<Object>>
        private var initialized: Boolean

        /**
         * Note that it only works for batch scenario. E.g. all data is known and there is no
         * late data.
         *
         * @param inputEnumerator the enumerator to provide an array of objects as input
         * @param indexOfWatermarkedColumn the index of timestamp column upon which a watermark is built
         * @param indexOfKeyColumn the index of column that acts as grouping key
         * @param gap gap parameter
         */
        init {
            this.inputEnumerator = inputEnumerator
            this.indexOfWatermarkedColumn = indexOfWatermarkedColumn
            this.indexOfKeyColumn = indexOfKeyColumn
            this.gap = gap
            list = ArrayDeque()
            initialized = false
        }

        @Override
        @Nullable
        fun current(): Array<Object> {
            if (!initialized) {
                initialize()
                initialized = true
            }
            return list.removeFirst()
        }

        @Override
        fun moveNext(): Boolean {
            return if (initialized) list.size() > 0 else inputEnumerator.moveNext()
        }

        @Override
        fun reset() {
            list.clear()
            inputEnumerator.reset()
            initialized = false
        }

        @Override
        fun close() {
            list.clear()
            inputEnumerator.close()
            initialized = false
        }

        private fun initialize() {
            val elements: List<Array<Object>> = ArrayList()
            // initialize() will be called when inputEnumerator.moveNext() is true,
            // thus firstly should take the current element.
            elements.add(inputEnumerator.current())
            // sessionization needs to see all data.
            while (inputEnumerator.moveNext()) {
                elements.add(inputEnumerator.current())
            }
            val sessionKeyMap: Map<Object, SortedMultiMap<Pair<Long, Long>, Array<Object>>> = HashMap()
            for (@Nullable element in elements) {
                val session: SortedMultiMap<Pair<Long, Long>, Array<Object>> = sessionKeyMap.computeIfAbsent(
                    element[indexOfKeyColumn]
                ) { k -> SortedMultiMap() }
                val watermark: Object = requireNonNull(
                    element[indexOfWatermarkedColumn],
                    "element[indexOfWatermarkedColumn]"
                )
                val initWindow: Pair<Long, Long> = computeInitWindow(
                    SqlFunctions.toLong(watermark), gap
                )
                session.putMulti(initWindow, element)
            }

            // merge per key session windows if there is any overlap between windows.
            for (perKeyEntry in sessionKeyMap.entrySet()) {
                val finalWindowElementsMap: Map<Pair<Long, Long>, List<Array<Object>>> = HashMap()
                var currentWindow: Pair<Long, Long>? = null
                val tempElementList: List<Array<Object>> = ArrayList()
                for (sessionEntry in perKeyEntry.getValue().entrySet()) {
                    // check the next window can be merged.
                    if (currentWindow == null || !isOverlapped(currentWindow, sessionEntry.getKey())) {
                        // cannot merge window as there is no overlap
                        if (currentWindow != null) {
                            finalWindowElementsMap.put(currentWindow, ArrayList(tempElementList))
                        }
                        currentWindow = sessionEntry.getKey()
                        tempElementList.clear()
                        tempElementList.addAll(sessionEntry.getValue())
                    } else {
                        // merge windows.
                        currentWindow = mergeWindows(currentWindow, sessionEntry.getKey())
                        // merge elements in windows.
                        tempElementList.addAll(sessionEntry.getValue())
                    }
                }
                if (!tempElementList.isEmpty()) {
                    requireNonNull(currentWindow, "currentWindow is null")
                    finalWindowElementsMap.put(currentWindow, ArrayList(tempElementList))
                }

                // construct final results from finalWindowElementsMap.
                for (finalWindowElementsEntry in finalWindowElementsMap.entrySet()) {
                    for (@Nullable element in finalWindowElementsEntry.getValue()) {
                        @Nullable val curWithWindow: Array<Object?> = arrayOfNulls<Object>(element.size + 2)
                        System.arraycopy(element, 0, curWithWindow, 0, element.size)
                        curWithWindow[element.size] = finalWindowElementsEntry.getKey().left
                        curWithWindow[element.size + 1] = finalWindowElementsEntry.getKey().right
                        list.offer(curWithWindow)
                    }
                }
            }
        }

        companion object {
            private fun isOverlapped(a: Pair<Long, Long>, b: Pair<Long, Long>): Boolean {
                return b.left < a.right
            }

            private fun mergeWindows(a: Pair<Long, Long>, b: Pair<Long, Long>): Pair<Long, Long> {
                return Pair(if (a.left <= b.left) a.left else b.left, if (a.right >= b.right) a.right else b.right)
            }

            private fun computeInitWindow(ts: Long, gap: Long): Pair<Long, Long> {
                return Pair(ts, ts + gap)
            }
        }
    }

    /** Enumerator that computes HOP.  */
    private class HopEnumerator internal constructor(
        inputEnumerator: Enumerator<Array<Object?>?>,
        indexOfWatermarkedColumn: Int, slide: Long, windowSize: Long, offset: Long
    ) : Enumerator<Array<Object?>?> {
        private val inputEnumerator: Enumerator<Array<Object>>
        private val indexOfWatermarkedColumn: Int
        private val emitFrequency: Long
        private val windowSize: Long
        private val offset: Long
        private val list: Deque<Array<Object>>

        /**
         * Note that it only works for batch scenario. E.g. all data is known and there is no late data.
         *
         * @param inputEnumerator the enumerator to provide an array of objects as input
         * @param indexOfWatermarkedColumn the index of timestamp column upon which a watermark is built
         * @param slide sliding size
         * @param windowSize window size
         * @param offset indicates how much windows should off
         */
        init {
            this.inputEnumerator = inputEnumerator
            this.indexOfWatermarkedColumn = indexOfWatermarkedColumn
            emitFrequency = slide
            this.windowSize = windowSize
            this.offset = offset
            list = ArrayDeque()
        }

        @Override
        @Nullable
        fun current(): Array<Object> {
            return if (list.size() > 0) {
                takeOne()
            } else {
                @Nullable val current: Array<Object> = inputEnumerator.current()
                val watermark: Object = requireNonNull(
                    current[indexOfWatermarkedColumn],
                    "element[indexOfWatermarkedColumn]"
                )
                val windows: List<Pair<Long, Long>> = hopWindows(
                    SqlFunctions.toLong(watermark),
                    emitFrequency, windowSize, offset
                )
                for (window in windows) {
                    @Nullable val curWithWindow: Array<Object?> = arrayOfNulls<Object>(current.size + 2)
                    System.arraycopy(current, 0, curWithWindow, 0, current.size)
                    curWithWindow[current.size] = window.left
                    curWithWindow[current.size + 1] = window.right
                    list.offer(curWithWindow)
                }
                takeOne()
            }
        }

        @Override
        fun moveNext(): Boolean {
            return list.size() > 0 || inputEnumerator.moveNext()
        }

        @Override
        fun reset() {
            inputEnumerator.reset()
            list.clear()
        }

        @Override
        fun close() {
        }

        @Nullable
        private fun takeOne(): Array<Object> {
            return requireNonNull(list.pollFirst(), "list.pollFirst()")
        }
    }
}
