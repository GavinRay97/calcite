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
 * Translates [REX expressions][org.apache.calcite.rex.RexNode] to
 * [linq4j expressions][Expression].
 */
class RexToLixTranslator private constructor(
    @Nullable program: RexProgram?,
    typeFactory: JavaTypeFactory,
    root: Expression,
    @Nullable inputGetter: InputGetter?,
    list: BlockBuilder,
    @Nullable staticList: BlockBuilder?,
    builder: RexBuilder,
    conformance: SqlConformance,
    @Nullable correlates: Function1<String, InputGetter>?
) : RexVisitor<RexToLixTranslator.Result?> {
    val typeFactory: JavaTypeFactory
    val builder: RexBuilder

    @Nullable
    private val program: RexProgram?
    val conformance: SqlConformance
    private val root: Expression
    val inputGetter: @Nullable InputGetter?
    private val list: BlockBuilder

    @Nullable
    private val staticList: BlockBuilder?

    @Nullable
    private val correlates: Function1<String, InputGetter>?

    /**
     * Map from RexLiteral's variable name to its literal, which is often a
     * ([org.apache.calcite.linq4j.tree.ConstantExpression]))
     * It is used in the some `RexCall`'s implementors, such as
     * `ExtractImplementor`.
     *
     * @see .getLiteral
     *
     * @see .getLiteralValue
     */
    private val literalMap: Map<Expression?, Expression> = HashMap()

    /** For `RexCall`, keep the list of its operand's `Result`.
     * It is useful when creating a `CallImplementor`.  */
    private val callOperandResultMap: Map<RexCall, List<Result>> = HashMap()

    /** Map from RexNode under specific storage type to its Result, to avoid
     * generating duplicate code. For `RexInputRef`, `RexDynamicParam`
     * and `RexFieldAccess`.  */
    private val rexWithStorageTypeResultMap: Map<Pair<RexNode, Type>, Result> = HashMap()

    /** Map from RexNode to its Result, to avoid generating duplicate code.
     * For `RexLiteral` and `RexCall`.  */
    private val rexResultMap: Map<RexNode, Result> = HashMap()

    @Nullable
    private var currentStorageType: Type? = null

    init {
        this.program = program // may be null
        this.typeFactory = requireNonNull(typeFactory, "typeFactory")
        this.conformance = requireNonNull(conformance, "conformance")
        this.root = requireNonNull(root, "root")
        this.inputGetter = inputGetter
        this.list = requireNonNull(list, "list")
        this.staticList = staticList
        this.builder = requireNonNull(builder, "builder")
        this.correlates = correlates // may be null
    }

    fun translate(expr: RexNode?): Expression {
        val nullAs: RexImpTable.NullAs = RexImpTable.NullAs.of(isNullable(expr))
        return translate(expr, nullAs)
    }

    fun translate(expr: RexNode, nullAs: RexImpTable.NullAs): Expression? {
        return translate(expr, nullAs, null)
    }

    fun translate(expr: RexNode, @Nullable storageType: Type?): Expression? {
        val nullAs: RexImpTable.NullAs = RexImpTable.NullAs.of(isNullable(expr))
        return translate(expr, nullAs, storageType)
    }

    fun translate(
        expr: RexNode, nullAs: RexImpTable.NullAs,
        @Nullable storageType: Type?
    ): Expression? {
        currentStorageType = storageType
        val result: Result = expr.accept(this)
        val translated: Expression = requireNonNull(EnumUtils.toInternal(result.valueVariable, storageType))
        // When we asked for not null input that would be stored as box, avoid unboxing
        return if (RexImpTable.NullAs.NOT_POSSIBLE === nullAs
            && translated.type.equals(storageType)
        ) {
            translated
        } else nullAs.handle(translated)
    }

    fun translateCast(
        sourceType: RelDataType,
        targetType: RelDataType,
        operand: Expression
    ): Expression? {
        var convert: Expression? = null
        when (targetType.getSqlTypeName()) {
            ANY -> convert = operand
            DATE -> convert = translateCastToDate(sourceType, operand)
            TIME -> convert = translateCastToTime(sourceType, operand)
            TIME_WITH_LOCAL_TIME_ZONE -> when (sourceType.getSqlTypeName()) {
                CHAR, VARCHAR -> convert =
                    Expressions.call(BuiltInMethod.STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method, operand)
                TIME -> convert = Expressions.call(
                    BuiltInMethod.TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                    RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIME_TO_STRING.method,
                            operand
                        )
                    ),
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
                TIMESTAMP -> convert = Expressions.call(
                    BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                    RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                            operand
                        )
                    ),
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                        operand
                    )
                )
                else -> {}
            }
            TIMESTAMP -> when (sourceType.getSqlTypeName()) {
                CHAR, VARCHAR -> convert = Expressions.call(BuiltInMethod.STRING_TO_TIMESTAMP.method, operand)
                DATE -> convert = Expressions.multiply(
                    Expressions.convert_(operand, Long::class.javaPrimitiveType),
                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                )
                TIME -> convert = Expressions.add(
                    Expressions.multiply(
                        Expressions.convert_(
                            Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                            Long::class.javaPrimitiveType
                        ),
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                    ),
                    Expressions.convert_(operand, Long::class.javaPrimitiveType)
                )
                TIME_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                        Expressions.call(
                            BuiltInMethod.UNIX_DATE_TO_STRING.method,
                            Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)
                        ),
                        operand,
                        Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                    )
                )
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                        operand,
                        Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                    )
                )
                else -> {}
            }
            TIMESTAMP_WITH_LOCAL_TIME_ZONE -> when (sourceType.getSqlTypeName()) {
                CHAR, VARCHAR -> convert = Expressions.call(
                    BuiltInMethod.STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                    operand
                )
                DATE -> convert = Expressions.call(
                    BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                    RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                            Expressions.multiply(
                                Expressions.convert_(operand, Long::class.javaPrimitiveType),
                                Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                            )
                        )
                    ),
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
                TIME -> convert = Expressions.call(
                    BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                    RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                            Expressions.add(
                                Expressions.multiply(
                                    Expressions.convert_(
                                        Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                                        Long::class.javaPrimitiveType
                                    ),
                                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                                ),
                                Expressions.convert_(operand, Long::class.javaPrimitiveType)
                            )
                        )
                    ),
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
                TIME_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                        Expressions.call(
                            BuiltInMethod.UNIX_DATE_TO_STRING.method,
                            Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)
                        ),
                        operand
                    )
                )
                TIMESTAMP -> convert = Expressions.call(
                    BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                    RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                            operand
                        )
                    ),
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
                else -> {}
            }
            BOOLEAN -> when (sourceType.getSqlTypeName()) {
                CHAR, VARCHAR -> convert = Expressions.call(
                    BuiltInMethod.STRING_TO_BOOLEAN.method,
                    operand
                )
                else -> {}
            }
            CHAR, VARCHAR -> {
                val interval: SqlIntervalQualifier = sourceType.getIntervalQualifier()
                when (sourceType.getSqlTypeName()) {
                    DATE -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_DATE_TO_STRING.method,
                            operand
                        )
                    )
                    TIME -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIME_TO_STRING.method,
                            operand
                        )
                    )
                    TIME_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                            operand,
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                        )
                    )
                    TIMESTAMP -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                            operand
                        )
                    )
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                            operand,
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                        )
                    )
                    INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                            operand,
                            Expressions.constant(requireNonNull(interval, "interval").timeUnitRange)
                        )
                    )
                    INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> convert =
                        RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method,
                                operand,
                                Expressions.constant(requireNonNull(interval, "interval").timeUnitRange),
                                Expressions.constant(
                                    interval.getFractionalSecondPrecision(
                                        typeFactory.getTypeSystem()
                                    )
                                )
                            )
                        )
                    BOOLEAN -> convert = RexImpTable.optimize2(
                        operand,
                        Expressions.call(
                            BuiltInMethod.BOOLEAN_TO_STRING.method,
                            operand
                        )
                    )
                    else -> {}
                }
            }
            else -> {}
        }
        if (convert == null) {
            convert = EnumUtils.convert(operand, typeFactory.getJavaClass(targetType))
        }
        // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
        // longer than n.
        var pad = false
        var truncate = true
        when (targetType.getSqlTypeName()) {
            CHAR, BINARY -> {
                pad = true
                val targetPrecision: Int = targetType.getPrecision()
                if (targetPrecision >= 0) {
                    when (sourceType.getSqlTypeName()) {
                        CHAR, VARCHAR, BINARY, VARBINARY -> {
                            // If this is a widening cast, no need to truncate.
                            val sourcePrecision: Int = sourceType.getPrecision()
                            if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
                                <= 0
                            ) {
                                truncate = false
                            }
                            // If this is a widening cast, no need to pad.
                            if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
                                >= 0
                            ) {
                                pad = false
                            }
                            if (truncate || pad) {
                                convert = Expressions.call(
                                    if (pad) BuiltInMethod.TRUNCATE_OR_PAD.method else BuiltInMethod.TRUNCATE.method,
                                    convert,
                                    Expressions.constant(targetPrecision)
                                )
                            }
                        }
                        else -> if (truncate || pad) {
                            convert = Expressions.call(
                                if (pad) BuiltInMethod.TRUNCATE_OR_PAD.method else BuiltInMethod.TRUNCATE.method,
                                convert,
                                Expressions.constant(targetPrecision)
                            )
                        }
                    }
                }
            }
            VARCHAR, VARBINARY -> {
                val targetPrecision: Int = targetType.getPrecision()
                if (targetPrecision >= 0) {
                    when (sourceType.getSqlTypeName()) {
                        CHAR, VARCHAR, BINARY, VARBINARY -> {
                            val sourcePrecision: Int = sourceType.getPrecision()
                            if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
                                <= 0
                            ) {
                                truncate = false
                            }
                            if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
                                >= 0
                            ) {
                                pad = false
                            }
                            if (truncate || pad) {
                                convert = Expressions.call(
                                    if (pad) BuiltInMethod.TRUNCATE_OR_PAD.method else BuiltInMethod.TRUNCATE.method,
                                    convert,
                                    Expressions.constant(targetPrecision)
                                )
                            }
                        }
                        else -> if (truncate || pad) {
                            convert = Expressions.call(
                                if (pad) BuiltInMethod.TRUNCATE_OR_PAD.method else BuiltInMethod.TRUNCATE.method,
                                convert,
                                Expressions.constant(targetPrecision)
                            )
                        }
                    }
                }
            }
            TIMESTAMP -> {
                var targetScale: Int = targetType.getScale()
                if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
                    targetScale = 0
                }
                if (targetScale < sourceType.getScale()) {
                    convert = Expressions.call(
                        BuiltInMethod.ROUND_LONG.method,
                        convert,
                        Expressions.constant(
                            Math.pow(10, 3 - targetScale) as Long
                        )
                    )
                }
            }
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> when (requireNonNull(
                sourceType.getSqlTypeName().getFamily()
            ) {
                ("null SqlTypeFamily for " + sourceType + ", SqlTypeName "
                        + sourceType.getSqlTypeName())
            }) {
                NUMERIC -> {
                    val multiplier: BigDecimal = targetType.getSqlTypeName().getEndUnit().multiplier
                    val divider: BigDecimal = BigDecimal.ONE
                    convert = RexImpTable.multiplyDivide(convert, multiplier, divider)
                }
                else -> {}
            }
            else -> {}
        }
        return scaleIntervalToNumber(sourceType, targetType, convert)
    }

    @Nullable
    private fun translateCastToTime(sourceType: RelDataType, operand: Expression): Expression? {
        var convert: Expression? = null
        when (sourceType.getSqlTypeName()) {
            CHAR, VARCHAR -> convert = Expressions.call(BuiltInMethod.STRING_TO_TIME.method, operand)
            TIME_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                    operand,
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
            )
            TIMESTAMP -> convert = Expressions.convert_(
                Expressions.call(
                    BuiltInMethod.FLOOR_MOD.method,
                    operand,
                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                ),
                Int::class.javaPrimitiveType
            )
            TIMESTAMP_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                    operand,
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
            )
            else -> {}
        }
        return convert
    }

    @Nullable
    private fun translateCastToDate(sourceType: RelDataType, operand: Expression): Expression? {
        var convert: Expression? = null
        when (sourceType.getSqlTypeName()) {
            CHAR, VARCHAR -> convert = Expressions.call(BuiltInMethod.STRING_TO_DATE.method, operand)
            TIMESTAMP -> convert = Expressions.convert_(
                Expressions.call(
                    BuiltInMethod.FLOOR_DIV.method,
                    operand, Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                ),
                Int::class.javaPrimitiveType
            )
            TIMESTAMP_WITH_LOCAL_TIME_ZONE -> convert = RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE.method,
                    operand,
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root)
                )
            )
            else -> {}
        }
        return convert
    }

    /**
     * Handle checked Exceptions declared in Method. In such case,
     * method call should be wrapped in a try...catch block.
     * "
     * final Type method_call;
     * try {
     * method_call = callExpr
     * } catch (Exception e) {
     * throw new RuntimeException(e);
     * }
     * "
     */
    fun handleMethodCheckedExceptions(callExpr: Expression): Expression {
        // Try statement
        val methodCall: ParameterExpression = Expressions.parameter(
            callExpr.getType(), list.newName("method_call")
        )
        list.add(Expressions.declare(Modifier.FINAL, methodCall, null))
        val st: Statement = Expressions.statement(Expressions.assign(methodCall, callExpr))
        // Catch Block, wrap checked exception in unchecked exception
        val e: ParameterExpression = Expressions.parameter(0, Exception::class.java, "e")
        val uncheckedException: Expression = Expressions.new_(RuntimeException::class.java, e)
        val cb: CatchBlock = Expressions.catch_(e, Expressions.throw_(uncheckedException))
        list.add(Expressions.tryCatch(st, cb))
        return methodCall
    }

    /** Dereferences an expression if it is a
     * [org.apache.calcite.rex.RexLocalRef].  */
    fun deref(expr: RexNode): RexNode {
        return if (expr is RexLocalRef) {
            val ref: RexLocalRef = expr as RexLocalRef
            val e2: RexNode = requireNonNull(program, "program")
                .getExprList().get(ref.getIndex())
            assert(ref.getType().equals(e2.getType()))
            e2
        } else {
            expr
        }
    }

    fun translateList(
        operandList: List<RexNode?>?,
        nullAs: RexImpTable.NullAs
    ): List<Expression> {
        return translateList(
            operandList, nullAs,
            EnumUtils.internalTypes(operandList)
        )
    }

    fun translateList(
        operandList: List<RexNode?>?,
        nullAs: RexImpTable.NullAs,
        storageTypes: List<Type?>?
    ): List<Expression> {
        val list: List<Expression> = ArrayList()
        for (e in Pair.zip(operandList, storageTypes)) {
            list.add(translate(e.left, nullAs, e.right))
        }
        return list
    }

    /**
     * Translates the list of `RexNode`, using the default output types.
     * This might be suboptimal in terms of additional box-unbox when you use
     * the translation later.
     * If you know the java class that will be used to store the results, use
     * [org.apache.calcite.adapter.enumerable.RexToLixTranslator.translateList]
     * version.
     *
     * @param operandList list of RexNodes to translate
     *
     * @return translated expressions
     */
    fun translateList(operandList: List<RexNode?>): List<Expression> {
        return translateList(operandList, EnumUtils.internalTypes(operandList))
    }

    /**
     * Translates the list of `RexNode`, while optimizing for output
     * storage.
     * For instance, if the result of translation is going to be stored in
     * `Object[]`, and the input is `Object[]` as well,
     * then translator will avoid casting, boxing, etc.
     *
     * @param operandList list of RexNodes to translate
     * @param storageTypes hints of the java classes that will be used
     * to store translation results. Use null to use
     * default storage type
     *
     * @return translated expressions
     */
    fun translateList(
        operandList: List<RexNode?>,
        @Nullable storageTypes: List<Type?>?
    ): List<Expression> {
        val list: List<Expression> = ArrayList(operandList.size())
        for (i in 0 until operandList.size()) {
            val rex: RexNode? = operandList[i]
            var desiredType: Type? = null
            if (storageTypes != null) {
                desiredType = storageTypes[i]
            }
            val translate: Expression = translate(rex, desiredType)
            list.add(translate)
            // desiredType is still a hint, thus we might get any kind of output
            // (boxed or not) when hint was provided.
            // It is favourable to get the type matching desired type
            if (desiredType == null && !isNullable(rex)) {
                assert(!Primitive.isBox(translate.getType())) {
                    ("Not-null boxed primitive should come back as primitive: "
                            + rex + ", " + translate.getType())
                }
            }
        }
        return list
    }

    private fun translateTableFunction(
        rexCall: RexCall, inputEnumerable: Expression,
        inputPhysType: PhysType, outputPhysType: PhysType
    ): Expression {
        assert(rexCall.getOperator() is SqlWindowTableFunction)
        val implementor: TableFunctionCallImplementor =
            RexImpTable.INSTANCE.get(rexCall.getOperator() as SqlWindowTableFunction)
                ?: throw Util.needToImplement("implementor of " + rexCall.getOperator().getName())
        return implementor.implement(
            this, inputEnumerable, rexCall, inputPhysType, outputPhysType
        )
    }

    /** Returns whether an expression is nullable.
     * @param e Expression
     * @return Whether expression is nullable
     */
    fun isNullable(e: RexNode?): Boolean {
        return e.getType().isNullable()
    }

    fun setBlock(list: BlockBuilder): RexToLixTranslator {
        return if (list === this.list) {
            this
        } else RexToLixTranslator(
            program, typeFactory, root, inputGetter, list,
            staticList, builder, conformance, correlates
        )
    }

    fun setCorrelates(
        @Nullable correlates: Function1<String?, InputGetter>
    ): RexToLixTranslator {
        return if (this.correlates === correlates) {
            this
        } else RexToLixTranslator(
            program, typeFactory, root, inputGetter, list,
            staticList, builder, conformance, correlates
        )
    }

    fun getRoot(): Expression {
        return root
    }

    /**
     * Visit `RexInputRef`. If it has never been visited
     * under current storage type before, `RexToLixTranslator`
     * generally produces three lines of code.
     * For example, when visiting a column (named commission) in
     * table Employee, the generated code snippet is:
     * `final Employee current =(Employee) inputEnumerator.current();
     * final Integer input_value = current.commission;
     * final boolean input_isNull = input_value == null;
    ` *
     */
    @Override
    fun visitInputRef(inputRef: RexInputRef): Result? {
        val key: Pair<RexNode, Type> = Pair.of(inputRef, currentStorageType)
        // If the RexInputRef has been visited under current storage type already,
        // it is not necessary to visit it again, just return the result.
        if (rexWithStorageTypeResultMap.containsKey(key)) {
            return rexWithStorageTypeResultMap[key]
        }
        // Generate one line of code to get the input, e.g.,
        // "final Employee current =(Employee) inputEnumerator.current();"
        val valueExpression: Expression = requireNonNull(inputGetter, "inputGetter").field(
            list, inputRef.getIndex(), currentStorageType
        )

        // Generate one line of code for the value of RexInputRef, e.g.,
        // "final Integer input_value = current.commission;"
        val valueVariable: ParameterExpression = Expressions.parameter(
            valueExpression.getType(), list.newName("input_value")
        )
        list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression))

        // Generate one line of code to check whether RexInputRef is null, e.g.,
        // "final boolean input_isNull = input_value == null;"
        val isNullExpression: Expression = checkNull(valueVariable)
        val isNullVariable: ParameterExpression = Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull")
        )
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression))
        val result = Result(isNullVariable, valueVariable)

        // Cache <RexInputRef, currentStorageType>'s result
        // Note: EnumerableMatch's PrevInputGetter changes index each time,
        // it is not right to reuse the result under such case.
        if (inputGetter !is EnumerableMatch.PrevInputGetter) {
            rexWithStorageTypeResultMap.put(key, result)
        }
        return Result(isNullVariable, valueVariable)
    }

    @Override
    fun visitLocalRef(localRef: RexLocalRef): Result {
        return deref(localRef).accept(this)
    }

    /**
     * Visit `RexLiteral`. If it has never been visited before,
     * `RexToLixTranslator` will generate two lines of code. For example,
     * when visiting a primitive int (10), the generated code snippet is:
     * `final int literal_value = 10;
     * final boolean literal_isNull = false;
    ` *
     */
    @Override
    fun visitLiteral(literal: RexLiteral): Result? {
        // If the RexLiteral has been visited already, just return the result
        if (rexResultMap.containsKey(literal)) {
            return rexResultMap[literal]
        }
        // Generate one line of code for the value of RexLiteral, e.g.,
        // "final int literal_value = 10;"
        val valueExpression: Expression =
            if (literal.isNull() // Note: even for null literal, we can't loss its type information
            ) getTypedNullLiteral(literal) else translateLiteral(
                literal, literal.getType(),
                typeFactory, RexImpTable.NullAs.NOT_POSSIBLE
            )
        val valueVariable: ParameterExpression
        val literalValue: Expression = appendConstant("literal_value", valueExpression)
        if (literalValue is ParameterExpression) {
            valueVariable = literalValue as ParameterExpression
        } else {
            valueVariable = Expressions.parameter(
                valueExpression.getType(),
                list.newName("literal_value")
            )
            list.add(
                Expressions.declare(Modifier.FINAL, valueVariable, valueExpression)
            )
        }

        // Generate one line of code to check whether RexLiteral is null, e.g.,
        // "final boolean literal_isNull = false;"
        val isNullExpression: Expression = if (literal.isNull()) RexImpTable.TRUE_EXPR else RexImpTable.FALSE_EXPR
        val isNullVariable: ParameterExpression = Expressions.parameter(
            Boolean.TYPE, list.newName("literal_isNull")
        )
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression))

        // Maintain the map from valueVariable (ParameterExpression) to real Expression
        literalMap.put(valueVariable, valueExpression)
        val result = Result(isNullVariable, valueVariable)
        // Cache RexLiteral's result
        rexResultMap.put(literal, result)
        return result
    }

    /**
     * Returns an `Expression` for null literal without losing its type
     * information.
     */
    private fun getTypedNullLiteral(literal: RexLiteral): ConstantExpression {
        assert(literal.isNull())
        var javaClass: Type? = typeFactory.getJavaClass(literal.getType())
        when (literal.getType().getSqlTypeName()) {
            DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> javaClass =
                Integer::class.java
            TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> javaClass =
                Long::class.java
            else -> {}
        }
        return if (javaClass == null || javaClass === Void::class.java) RexImpTable.NULL_EXPR else Expressions.constant(
            null,
            javaClass
        )
    }

    /**
     * Visit `RexCall`. For most `SqlOperator`s, we can get the implementor
     * from `RexImpTable`. Several operators (e.g., CaseWhen) with special semantics
     * need to be implemented separately.
     */
    @Override
    fun visitCall(call: RexCall): Result? {
        if (rexResultMap.containsKey(call)) {
            return rexResultMap[call]
        }
        val operator: SqlOperator = call.getOperator()
        if (operator === PREV) {
            return implementPrev(call)
        }
        if (operator === CASE) {
            return implementCaseWhen(call)
        }
        if (operator === SEARCH) {
            return RexUtil.expandSearch(builder, program, call).accept(this)
        }
        val implementor: RexImpTable.RexCallImplementor = RexImpTable.INSTANCE.get(operator)
            ?: throw RuntimeException("cannot translate call $call")
        val operandList: List<RexNode> = call.getOperands()
        val storageTypes: List<Type> = EnumUtils.internalTypes(operandList)
        val operandResults: List<Result> = ArrayList()
        for (i in 0 until operandList.size()) {
            val operandResult = implementCallOperand(
                operandList[i], storageTypes[i], this
            )
            operandResults.add(operandResult)
        }
        callOperandResultMap.put(call, operandResults)
        val result: Result = implementor.implement(this, call, operandResults)
        rexResultMap.put(call, result)
        return result
    }

    /**
     * For `PREV` operator, the offset of `inputGetter`
     * should be set first.
     */
    private fun implementPrev(call: RexCall): Result {
        val node: RexNode = call.getOperands().get(0)
        val offset: RexNode = call.getOperands().get(1)
        val offs: Expression = Expressions.multiply(
            translate(offset),
            Expressions.constant(-1)
        )
        requireNonNull(inputGetter as EnumerableMatch.PrevInputGetter?, "inputGetter")
            .setOffset(offs)
        return node.accept(this)
    }

    /**
     * The CASE operator is SQL’s way of handling if/then logic.
     * Different with other `RexCall`s, it is not safe to
     * implement its operands first.
     * For example: `select case when s=0 then false
     * else 100/s > 0 end
     * from (values (1),(0)) ax(s);
    ` *
     */
    private fun implementCaseWhen(call: RexCall): Result {
        val returnType: Type = typeFactory.getJavaClass(call.getType())
        val valueVariable: ParameterExpression = Expressions.parameter(
            returnType,
            list.newName("case_when_value")
        )
        list.add(Expressions.declare(0, valueVariable, null))
        val operandList: List<RexNode> = call.getOperands()
        implementRecursively(this, operandList, valueVariable, 0)
        val isNullExpression: Expression = checkNull(valueVariable)
        val isNullVariable: ParameterExpression = Expressions.parameter(
            Boolean.TYPE, list.newName("case_when_isNull")
        )
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression))
        val result = Result(isNullVariable, valueVariable)
        rexResultMap.put(call, result)
        return result
    }

    private fun toInnerStorageType(result: Result, storageType: Type): Result {
        val valueExpression: Expression = EnumUtils.toInternal(result.valueVariable, storageType)
        if (valueExpression.equals(result.valueVariable)) {
            return result
        }
        val valueVariable: ParameterExpression = Expressions.parameter(
            valueExpression.getType(),
            list.newName(result.valueVariable.name + "_inner_type")
        )
        list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression))
        val isNullVariable: ParameterExpression = result.isNullVariable
        return Result(isNullVariable, valueVariable)
    }

    @Override
    fun visitDynamicParam(dynamicParam: RexDynamicParam): Result? {
        val key: Pair<RexNode, Type> = Pair.of(dynamicParam, currentStorageType)
        if (rexWithStorageTypeResultMap.containsKey(key)) {
            return rexWithStorageTypeResultMap[key]
        }
        val storageType: Type =
            if (currentStorageType != null) currentStorageType else typeFactory.getJavaClass(dynamicParam.getType())
        val valueExpression: Expression = EnumUtils.convert(
            Expressions.call(
                root, BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant("?" + dynamicParam.getIndex())
            ),
            storageType
        )
        val valueVariable: ParameterExpression =
            Expressions.parameter(valueExpression.getType(), list.newName("value_dynamic_param"))
        list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression))
        val isNullVariable: ParameterExpression =
            Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"))
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, checkNull(valueVariable)))
        val result = Result(isNullVariable, valueVariable)
        rexWithStorageTypeResultMap.put(key, result)
        return result
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess): Result? {
        val key: Pair<RexNode, Type> = Pair.of(fieldAccess, currentStorageType)
        if (rexWithStorageTypeResultMap.containsKey(key)) {
            return rexWithStorageTypeResultMap[key]
        }
        val target: RexNode = deref(fieldAccess.getReferenceExpr())
        val fieldIndex: Int = fieldAccess.getField().getIndex()
        val fieldName: String = fieldAccess.getField().getName()
        return when (target.getKind()) {
            CORREL_VARIABLE -> {
                if (correlates == null) {
                    throw RuntimeException(
                        "Cannot translate " + fieldAccess
                                + " since correlate variables resolver is not defined"
                    )
                }
                val getter: InputGetter =
                    correlates.apply((target as RexCorrelVariable).getName())
                val input: Expression = getter.field(
                    list, fieldIndex, currentStorageType
                )
                val condition: Expression = checkNull(input)
                val valueVariable: ParameterExpression =
                    Expressions.parameter(input.getType(), list.newName("corInp_value"))
                list.add(Expressions.declare(Modifier.FINAL, valueVariable, input))
                val isNullVariable: ParameterExpression =
                    Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"))
                val isNullExpression: Expression = Expressions.condition(
                    condition,
                    RexImpTable.TRUE_EXPR,
                    checkNull(valueVariable)
                )
                list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression))
                val result1 = Result(isNullVariable, valueVariable)
                rexWithStorageTypeResultMap.put(key, result1)
                result1
            }
            else -> {
                val rxIndex: RexNode =
                    builder.makeLiteral(fieldIndex, typeFactory.createType(Int::class.javaPrimitiveType), true)
                val rxName: RexNode = builder.makeLiteral(fieldName, typeFactory.createType(String::class.java), true)
                val accessCall: RexCall = builder.makeCall(
                    fieldAccess.getType(), SqlStdOperatorTable.STRUCT_ACCESS,
                    ImmutableList.of(target, rxIndex, rxName)
                ) as RexCall
                val result2: Result = accessCall.accept(this)
                rexWithStorageTypeResultMap.put(key, result2)
                result2
            }
        }
    }

    @Override
    fun visitOver(over: RexOver): Result {
        throw RuntimeException("cannot translate expression $over")
    }

    @Override
    fun visitCorrelVariable(correlVariable: RexCorrelVariable): Result {
        throw RuntimeException(
            "Cannot translate " + correlVariable
                    + ". Correlated variables should always be referenced by field access"
        )
    }

    @Override
    fun visitRangeRef(rangeRef: RexRangeRef): Result {
        throw RuntimeException("cannot translate expression $rangeRef")
    }

    @Override
    fun visitSubQuery(subQuery: RexSubQuery): Result {
        throw RuntimeException("cannot translate expression $subQuery")
    }

    @Override
    fun visitTableInputRef(fieldRef: RexTableInputRef): Result {
        throw RuntimeException("cannot translate expression $fieldRef")
    }

    @Override
    fun visitPatternFieldRef(fieldRef: RexPatternFieldRef): Result? {
        return visitInputRef(fieldRef)
    }

    fun checkNull(expr: Expression?): Expression {
        return if (Primitive.flavor(expr.getType())
            === Primitive.Flavor.PRIMITIVE
        ) {
            RexImpTable.FALSE_EXPR
        } else Expressions.equal(expr, RexImpTable.NULL_EXPR)
    }

    fun checkNotNull(expr: Expression): Expression {
        return if (Primitive.flavor(expr.getType())
            === Primitive.Flavor.PRIMITIVE
        ) {
            RexImpTable.TRUE_EXPR
        } else Expressions.notEqual(expr, RexImpTable.NULL_EXPR)
    }

    val blockBuilder: BlockBuilder
        get() = list

    fun getLiteral(literalVariable: Expression): Expression {
        return requireNonNull(
            literalMap[literalVariable]
        ) { "literalMap.get(literalVariable) for $literalVariable" }
    }

    /** Returns the value of a literal.  */
    @Nullable
    fun getLiteralValue(@Nullable expr: Expression?): Object? {
        if (expr is ParameterExpression) {
            val constantExpr: Expression? = literalMap[expr]
            return getLiteralValue(constantExpr)
        }
        return if (expr is ConstantExpression) {
            (expr as ConstantExpression?).value
        } else null
    }

    fun getCallOperandResult(call: RexCall): List<Result> {
        return requireNonNull(
            callOperandResultMap[call]
        ) { "callOperandResultMap.get(call) for $call" }
    }

    /** Returns an expression that yields the function object whose method
     * we are about to call.
     *
     *
     * It might be 'new MyFunction()', but it also might be a reference
     * to a static field 'F', defined by
     * 'static final MyFunction F = new MyFunction()'.
     *
     *
     * If there is a constructor that takes a [FunctionContext]
     * argument, we call that, passing in the values of arguments that are
     * literals; this allows the function to do some computation at load time.
     *
     *
     * If the call is "f(1, 2 + 3, 'foo')" and "f" is implemented by method
     * "eval(int, int, String)" in "class MyFun", the expression might be
     * "new MyFunction(FunctionContexts.of(new Object[] {1, null, "foo"})".
     *
     * @param method Method that implements the UDF
     * @param call Call to the UDF
     * @return New expression
     */
    fun functionInstance(call: RexCall?, method: Method): Expression {
        val callBinding: RexCallBinding = RexCallBinding.create(typeFactory, call, program, ImmutableList.of())
        val target: Expression = getInstantiationExpression(method, callBinding)
        return appendConstant("f", target)
    }

    /** Helper for [.functionInstance].  */
    private fun getInstantiationExpression(
        method: Method,
        callBinding: RexCallBinding
    ): Expression {
        val declaringClass: Class<*> = method.getDeclaringClass()
        // If the UDF class has a constructor that takes a Context argument,
        // use that.
        try {
            val constructor: Constructor<*> = declaringClass.getConstructor(FunctionContext::class.java)
            val constantArgs: List<Expression> = ArrayList()
            Ord.forEach(
                method.getParameterTypes()
            ) { parameterType, i ->
                constantArgs.add(
                    if (callBinding.isOperandLiteral(i, true)) appendConstant(
                        "_arg",
                        Expressions.constant(
                            callBinding.getOperandLiteralValue(
                                i,
                                Primitive.box(parameterType)
                            )
                        )
                    ) else Expressions.constant(null)
                )
            }
            val context: Expression = Expressions.call(
                BuiltInMethod.FUNCTION_CONTEXTS_OF.method,
                DataContext.ROOT,
                Expressions.newArrayInit(Object::class.java, constantArgs)
            )
            return Expressions.new_(constructor, context)
        } catch (e: NoSuchMethodException) {
            // ignore
        }
        // The UDF class must have a public zero-args constructor.
        // Assume that the validator checked already.
        return Expressions.new_(declaringClass)
    }

    /** Stores a constant expression in a variable.  */
    private fun appendConstant(name: String, e: Expression): Expression {
        return if (staticList != null) {
            // If name is "camelCase", upperName is "CAMEL_CASE".
            val upperName: String = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name)
            staticList.append(upperName, e)
        } else {
            list.append(name, e)
        }
    }

    /** Translates a field of an input to an expression.  */
    interface InputGetter {
        fun field(list: BlockBuilder?, index: Int, @Nullable storageType: Type?): Expression
    }

    /** Implementation of [InputGetter] that calls
     * [PhysType.fieldReference].  */
    class InputGetterImpl(inputs: Map<Expression?, PhysType?>?) : InputGetter {
        private val inputs: ImmutableMap<Expression, PhysType>

        @Deprecated // to be removed before 2.0
        constructor(inputs: List<Pair<Expression, PhysType>?>) : this(mapOf(inputs)) {
        }

        constructor(e: Expression?, physType: PhysType?) : this(ImmutableMap.of(e, physType)) {}

        init {
            this.inputs = ImmutableMap.copyOf(inputs)
        }

        @Override
        override fun field(list: BlockBuilder, index: Int, @Nullable storageType: Type?): Expression {
            var offset = 0
            for (input in inputs.entrySet()) {
                val physType: PhysType = input.getValue()
                val fieldCount: Int = physType.getRowType().getFieldCount()
                if (index >= offset + fieldCount) {
                    offset += fieldCount
                    continue
                }
                val left: Expression = list.append("current", input.getKey())
                return physType.fieldReference(left, index - offset, storageType)
            }
            throw IllegalArgumentException("Unable to find field #$index")
        }

        companion object {
            private fun <K, V> mapOf(
                entries: Iterable<Map.Entry<K, V>?>
            ): Map<K, V> {
                val b: ImmutableMap.Builder<K, V> = ImmutableMap.builder()
                Pair.forEach(entries, b::put)
                return b.build()
            }
        }
    }

    /** Result of translating a `RexNode`.  */
    class Result(
        isNullVariable: ParameterExpression,
        valueVariable: ParameterExpression
    ) {
        val isNullVariable: ParameterExpression
        val valueVariable: ParameterExpression

        init {
            this.isNullVariable = isNullVariable
            this.valueVariable = valueVariable
        }
    }

    companion object {
        val JAVA_TO_SQL_METHOD_MAP: Map<Method, SqlOperator> =
            ImmutableMap.< Method, SqlOperator>builder<Method?, SqlOperator?>()
        .put(org.apache.calcite.adapter.enumerable.RexToLixTranslator.Companion.findMethod(String::
        class.java, "toUpperCase"), UPPER)
        .put(BuiltInMethod.SUBSTRING.method, SUBSTRING)
        .put(BuiltInMethod.OCTET_LENGTH.method, OCTET_LENGTH)
        .put(BuiltInMethod.CHAR_LENGTH.method, CHAR_LENGTH)
        .put(BuiltInMethod.TRANSLATE3.method, TRANSLATE3)
        .build()
        private fun findMethod(
            clazz: Class<*>, name: String, vararg parameterTypes: Class
        ): Method {
            return try {
                clazz.getMethod(name, parameterTypes)
            } catch (e: NoSuchMethodException) {
                throw RuntimeException(e)
            }
        }

        /**
         * Translates a [RexProgram] to a sequence of expressions and
         * declarations.
         *
         * @param program Program to be translated
         * @param typeFactory Type factory
         * @param conformance SQL conformance
         * @param list List of statements, populated with declarations
         * @param staticList List of member declarations
         * @param outputPhysType Output type, or null
         * @param root Root expression
         * @param inputGetter Generates expressions for inputs
         * @param correlates Provider of references to the values of correlated
         * variables
         * @return Sequence of expressions, optional condition
         */
        fun translateProjects(
            program: RexProgram,
            typeFactory: JavaTypeFactory, conformance: SqlConformance,
            list: BlockBuilder, @Nullable staticList: BlockBuilder?,
            @Nullable outputPhysType: PhysType?, root: Expression,
            inputGetter: InputGetter?, @Nullable correlates: Function1<String?, InputGetter>
        ): List<Expression> {
            var storageTypes: List<Type?>? = null
            if (outputPhysType != null) {
                val rowType: RelDataType = outputPhysType.getRowType()
                storageTypes = ArrayList(rowType.getFieldCount())
                for (i in 0 until rowType.getFieldCount()) {
                    storageTypes.add(outputPhysType.getJavaFieldType(i))
                }
            }
            return RexToLixTranslator(
                program, typeFactory, root, inputGetter,
                list, staticList, RexBuilder(typeFactory), conformance, null
            )
                .setCorrelates(correlates)
                .translateList(program.getProjectList(), storageTypes)
        }

        @Deprecated // to be removed before 2.0
        fun translateProjects(
            program: RexProgram,
            typeFactory: JavaTypeFactory, conformance: SqlConformance,
            list: BlockBuilder, @Nullable outputPhysType: PhysType?, root: Expression,
            inputGetter: InputGetter?, @Nullable correlates: Function1<String?, InputGetter>
        ): List<Expression> {
            return translateProjects(
                program, typeFactory, conformance, list, null,
                outputPhysType, root, inputGetter, correlates
            )
        }

        fun translateTableFunction(
            typeFactory: JavaTypeFactory,
            conformance: SqlConformance, list: BlockBuilder,
            root: Expression, rexCall: RexCall, inputEnumerable: Expression,
            inputPhysType: PhysType, outputPhysType: PhysType
        ): Expression {
            return RexToLixTranslator(
                null, typeFactory, root, null, list,
                null, RexBuilder(typeFactory), conformance, null
            )
                .translateTableFunction(rexCall, inputEnumerable, inputPhysType, outputPhysType)
        }

        /** Creates a translator for translating aggregate functions.  */
        fun forAggregation(
            typeFactory: JavaTypeFactory,
            list: BlockBuilder, @Nullable inputGetter: InputGetter?, conformance: SqlConformance
        ): RexToLixTranslator {
            val root: ParameterExpression = DataContext.ROOT
            return RexToLixTranslator(
                null, typeFactory, root, inputGetter, list,
                null, RexBuilder(typeFactory), conformance, null
            )
        }

        /** Translates a literal.
         *
         * @throws ControlFlowException if literal is null but `nullAs` is
         * [org.apache.calcite.adapter.enumerable.RexImpTable.NullAs.NOT_POSSIBLE].
         */
        fun translateLiteral(
            literal: RexLiteral,
            type: RelDataType?,
            typeFactory: JavaTypeFactory,
            nullAs: RexImpTable.NullAs?
        ): Expression {
            if (literal.isNull()) {
                return when (nullAs) {
                    TRUE, IS_NULL -> RexImpTable.TRUE_EXPR
                    FALSE, IS_NOT_NULL -> RexImpTable.FALSE_EXPR
                    NOT_POSSIBLE -> throw ControlFlowException()
                    NULL -> RexImpTable.NULL_EXPR
                    else -> RexImpTable.NULL_EXPR
                }
            } else {
                when (nullAs) {
                    IS_NOT_NULL -> return RexImpTable.TRUE_EXPR
                    IS_NULL -> return RexImpTable.FALSE_EXPR
                    else -> {}
                }
            }
            var javaClass: Type = typeFactory.getJavaClass(type)
            val value2: Object
            when (literal.getType().getSqlTypeName()) {
                DECIMAL -> {
                    val bd: BigDecimal = literal.getValueAs(BigDecimal::class.java)
                    if (javaClass === Float::class.javaPrimitiveType) {
                        return Expressions.constant(bd, javaClass)
                    } else if (javaClass === Double::class.javaPrimitiveType) {
                        return Expressions.constant(bd, javaClass)
                    }
                    assert(javaClass === BigDecimal::class.java)
                    return Expressions.new_(
                        BigDecimal::class.java,
                        Expressions.constant(
                            requireNonNull(
                                bd
                            ) { "value for $literal" }.toString()
                        )
                    )
                }
                DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> {
                    value2 = literal.getValueAs(Integer::class.java)
                    javaClass = Int::class.javaPrimitiveType
                }
                TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                    value2 = literal.getValueAs(Long::class.java)
                    javaClass = Long::class.javaPrimitiveType
                }
                CHAR, VARCHAR -> value2 = literal.getValueAs(String::class.java)
                BINARY, VARBINARY -> return Expressions.new_(
                    ByteString::class.java,
                    Expressions.constant(
                        literal.getValueAs(ByteArray::class.java),
                        ByteArray::class.java
                    )
                )
                GEOMETRY -> {
                    val geom: Geometries.Geom = requireNonNull(
                        literal.getValueAs(Geometries.Geom::class.java)
                    ) { "getValueAs(Geometries.Geom) for $literal" }
                    val wkt: String = GeoFunctions.ST_AsWKT(geom)
                    return Expressions.call(
                        null, BuiltInMethod.ST_GEOM_FROM_TEXT.method,
                        Expressions.constant(wkt)
                    )
                }
                SYMBOL -> {
                    value2 = requireNonNull(
                        literal.getValueAs(Enum::class.java)
                    ) { "getValueAs(Enum.class) for $literal" }
                    javaClass = value2.getClass()
                }
                else -> {
                    val primitive: Primitive = Primitive.ofBoxOr(javaClass)
                    val value: Comparable = literal.getValueAs(Comparable::class.java)
                    value2 = if (primitive != null && value is Number) {
                        primitive.number(value as Number)
                    } else {
                        value
                    }
                }
            }
            return Expressions.constant(value2, javaClass)
        }

        fun translateCondition(
            program: RexProgram,
            typeFactory: JavaTypeFactory?, list: BlockBuilder?, inputGetter: InputGetter?,
            correlates: Function1<String?, InputGetter>, conformance: SqlConformance?
        ): Expression {
            val condition: RexLocalRef = program.getCondition() ?: return RexImpTable.TRUE_EXPR
            val root: ParameterExpression = DataContext.ROOT
            var translator = RexToLixTranslator(
                program, typeFactory, root, inputGetter, list,
                null, RexBuilder(typeFactory), conformance, null
            )
            translator = translator.setCorrelates(correlates)
            return translator.translate(
                condition,
                RexImpTable.NullAs.FALSE
            )
        }

        private fun scaleIntervalToNumber(
            sourceType: RelDataType,
            targetType: RelDataType,
            operand: Expression?
        ): Expression? {
            when (requireNonNull(
                targetType.getSqlTypeName().getFamily()
            ) { "SqlTypeFamily for $targetType" }) {
                NUMERIC -> when (sourceType.getSqlTypeName()) {
                    INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                        // Scale to the given field.
                        val multiplier: BigDecimal = BigDecimal.ONE
                        val divider: BigDecimal = sourceType.getSqlTypeName().getEndUnit().multiplier
                        return RexImpTable.multiplyDivide(operand, multiplier, divider)
                    }
                    else -> {}
                }
                else -> {}
            }
            return operand
        }

        private fun implementCallOperand(
            operand: RexNode,
            @Nullable storageType: Type?, translator: RexToLixTranslator
        ): Result {
            val originalStorageType: Type? = translator.currentStorageType
            translator.currentStorageType = storageType
            var operandResult: Result = operand.accept(translator)
            if (storageType != null) {
                operandResult = translator.toInnerStorageType(operandResult, storageType)
            }
            translator.currentStorageType = originalStorageType
            return operandResult
        }

        private fun implementCallOperand2(
            operand: RexNode,
            @Nullable storageType: Type, translator: RexToLixTranslator
        ): Expression {
            val originalStorageType: Type? = translator.currentStorageType
            translator.currentStorageType = storageType
            val result: Expression = translator.translate(operand)
            translator.currentStorageType = originalStorageType
            return result
        }

        /**
         * Case statements of the form:
         * `CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END`.
         * When `a = true`, returns `b`;
         * when `c = true`, returns `d`;
         * else returns `e`.
         *
         *
         * We generate code that looks like:
         *
         * <blockquote><pre>`int case_when_value;
         * ......code for a......
         * if (!a_isNull && a_value) {
         * ......code for b......
         * case_when_value = res(b_isNull, b_value);
         * } else {
         * ......code for c......
         * if (!c_isNull && c_value) {
         * ......code for d......
         * case_when_value = res(d_isNull, d_value);
         * } else {
         * ......code for e......
         * case_when_value = res(e_isNull, e_value);
         * }
         * }
        `</pre></blockquote> *
         */
        private fun implementRecursively(
            currentTranslator: RexToLixTranslator,
            operandList: List<RexNode>, valueVariable: ParameterExpression, pos: Int
        ) {
            val currentBlockBuilder: BlockBuilder = currentTranslator.blockBuilder
            val storageTypes: List<Type> = EnumUtils.internalTypes(operandList)
            // [ELSE] clause
            if (pos == operandList.size() - 1) {
                val res: Expression = implementCallOperand2(
                    operandList[pos],
                    storageTypes[pos], currentTranslator
                )
                currentBlockBuilder.add(
                    Expressions.statement(
                        Expressions.assign(
                            valueVariable,
                            EnumUtils.convert(res, valueVariable.getType())
                        )
                    )
                )
                return
            }
            // Condition code: !a_isNull && a_value
            val testerNode: RexNode = operandList[pos]
            val testerResult = implementCallOperand(
                testerNode,
                storageTypes[pos], currentTranslator
            )
            val tester: Expression = Expressions.andAlso(
                Expressions.not(testerResult.isNullVariable),
                testerResult.valueVariable
            )
            // Code for {if} branch
            val ifTrueNode: RexNode = operandList[pos + 1]
            val ifTrueBlockBuilder = BlockBuilder(true, currentBlockBuilder)
            val ifTrueTranslator = currentTranslator.setBlock(ifTrueBlockBuilder)
            val ifTrueRes: Expression = implementCallOperand2(
                ifTrueNode,
                storageTypes[pos + 1], ifTrueTranslator
            )
            // Assign the value: case_when_value = ifTrueRes
            ifTrueBlockBuilder.add(
                Expressions.statement(
                    Expressions.assign(
                        valueVariable,
                        EnumUtils.convert(ifTrueRes, valueVariable.getType())
                    )
                )
            )
            val ifTrue: BlockStatement = ifTrueBlockBuilder.toBlock()
            // There is no [ELSE] clause
            if (pos + 1 == operandList.size() - 1) {
                currentBlockBuilder.add(
                    Expressions.ifThen(tester, ifTrue)
                )
                return
            }
            // Generate code for {else} branch recursively
            val ifFalseBlockBuilder = BlockBuilder(true, currentBlockBuilder)
            val ifFalseTranslator = currentTranslator.setBlock(ifFalseBlockBuilder)
            implementRecursively(ifFalseTranslator, operandList, valueVariable, pos + 2)
            val ifFalse: BlockStatement = ifFalseBlockBuilder.toBlock()
            currentBlockBuilder.add(
                Expressions.ifThenElse(tester, ifTrue, ifFalse)
            )
        }
    }
}
