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

/** Implementation of [PhysType].  */
class PhysTypeImpl internal constructor(
    typeFactory: JavaTypeFactory,
    rowType: RelDataType,
    javaRowClass: Type,
    format: JavaRowFormat
) : PhysType {
    private val typeFactory: JavaTypeFactory
    private override val rowType: RelDataType
    private val javaRowClass: Type
    private val fieldClasses: List<Class> = ArrayList()
    override val format: JavaRowFormat

    /** Creates a PhysTypeImpl.  */
    init {
        this.typeFactory = typeFactory
        this.rowType = rowType
        this.javaRowClass = javaRowClass
        this.format = format
        for (field in rowType.getFieldList()) {
            val fieldType: Type = typeFactory.getJavaClass(field.getType())
            fieldClasses.add(if (fieldType is Class) fieldType as Class else Array<Object>::class.java)
        }
    }

    @Override
    fun getFormat(): JavaRowFormat {
        return format
    }

    @Override
    fun project(integers: List<Integer?>, format: JavaRowFormat): PhysType {
        return project(integers, false, format)
    }

    @Override
    fun project(
        integers: List<Integer?>, indicator: Boolean,
        format: JavaRowFormat
    ): PhysType {
        val builder: RelDataTypeFactory.Builder = typeFactory.builder()
        for (index in integers) {
            builder.add(rowType.getFieldList().get(index))
        }
        if (indicator) {
            val booleanType: RelDataType = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN), false
            )
            for (index in integers) {
                builder.add(
                    "i$" + rowType.getFieldList().get(index).getName(),
                    booleanType
                )
            }
        }
        val projectedRowType: RelDataType = builder.build()
        return of(typeFactory, projectedRowType, format.optimize(projectedRowType))
    }

    @Override
    fun generateSelector(
        parameter: ParameterExpression?,
        fields: List<Integer>
    ): Expression {
        return generateSelector(parameter, fields, format)
    }

    @Override
    fun generateSelector(
        parameter: ParameterExpression?,
        fields: List<Integer>,
        targetFormat: JavaRowFormat
    ): Expression {
        // Optimize target format
        var targetFormat: JavaRowFormat = targetFormat
        when (fields.size()) {
            0 -> targetFormat = JavaRowFormat.LIST
            1 -> targetFormat = JavaRowFormat.SCALAR
            else -> {}
        }
        val targetPhysType: PhysType = project(fields, targetFormat)
        return when (format) {
            SCALAR -> Expressions.call(BuiltInMethod.IDENTITY_SELECTOR.method)
            else -> Expressions.lambda(
                Function1::class.java,
                targetPhysType.record(fieldReferences(parameter, fields)), parameter
            )
        }
    }

    @Override
    fun generateSelector(
        parameter: ParameterExpression?,
        fields: List<Integer?>, usedFields: List<Integer?>,
        targetFormat: JavaRowFormat
    ): Expression {
        val targetPhysType: PhysType = project(fields, true, targetFormat)
        val expressions: List<Expression> = ArrayList()
        for (ord in Ord.zip(fields)) {
            val field: Integer = ord.e
            if (usedFields.contains(field)) {
                expressions.add(fieldReference(parameter, field))
            } else {
                val primitive: Primitive = Primitive.of(targetPhysType.fieldClass(ord.i))
                expressions.add(
                    Expressions.constant(
                        if (primitive != null) primitive.defaultValue else null
                    )
                )
            }
        }
        for (field in fields) {
            expressions.add(Expressions.constant(!usedFields.contains(field)))
        }
        return Expressions.lambda(
            Function1::class.java,
            targetPhysType.record(expressions), parameter
        )
    }

    @Override
    fun selector(
        parameter: ParameterExpression,
        fields: List<Integer>,
        targetFormat: JavaRowFormat
    ): Pair<Type, List<Expression>> {
        // Optimize target format
        var targetFormat: JavaRowFormat = targetFormat
        when (fields.size()) {
            0 -> targetFormat = JavaRowFormat.LIST
            1 -> targetFormat = JavaRowFormat.SCALAR
            else -> {}
        }
        val targetPhysType: PhysType = project(fields, targetFormat)
        return when (format) {
            SCALAR -> Pair.of(parameter.getType(), ImmutableList.of(parameter))
            else -> Pair.of(
                targetPhysType.getJavaRowType(),
                fieldReferences(parameter, fields)
            )
        }
    }

    @Override
    fun accessors(v1: Expression?, argList: List<Integer?>): List<Expression> {
        val expressions: List<Expression> = ArrayList()
        for (field in argList) {
            expressions.add(
                EnumUtils.convert(
                    fieldReference(v1, field),
                    fieldClass(field)
                )
            )
        }
        return expressions
    }

    @Override
    override fun makeNullable(nullable: Boolean): PhysType {
        return if (!nullable) {
            this
        } else PhysTypeImpl(
            typeFactory,
            typeFactory.createTypeWithNullability(rowType, true),
            Primitive.box(javaRowClass), format
        )
    }

    @SuppressWarnings("deprecation")
    @Override
    fun convertTo(exp: Expression?, targetPhysType: PhysType): Expression? {
        return convertTo(exp, targetPhysType.getFormat())
    }

    @Override
    fun convertTo(exp: Expression?, targetFormat: JavaRowFormat): Expression? {
        if (format === targetFormat) {
            return exp
        }
        val o_: ParameterExpression = Expressions.parameter(javaRowClass, "o")
        val fieldCount: Int = rowType.getFieldCount()
        // The conversion must be strict so optimizations of the targetFormat should not be performed
        // by the code that follows. If necessary the target format can be optimized before calling
        // this method.
        val targetPhysType: PhysType = of(typeFactory, rowType, targetFormat, false)
        val selector: Expression = Expressions.lambda(
            Function1::class.java,
            targetPhysType.record(fieldReferences(o_, Util.range(fieldCount))), o_
        )
        return Expressions.call(exp, BuiltInMethod.SELECT.method, selector)
    }

    @Override
    fun generateCollationKey(
        collations: List<RelFieldCollation>
    ): Pair<Expression, Expression> {
        val selector: Expression
        if (collations.size() === 1) {
            val collation: RelFieldCollation = collations[0]
            val fieldType: RelDataType = if (rowType.getFieldList() == null || rowType.getFieldList()
                    .isEmpty()
            ) rowType else rowType.getFieldList().get(collation.getFieldIndex()).getType()
            val fieldComparator: Expression = generateCollatorExpression(fieldType.getCollation())
            val parameter: ParameterExpression = Expressions.parameter(javaRowClass, "v")
            selector = Expressions.lambda(
                Function1::class.java,
                fieldReference(parameter, collation.getFieldIndex()),
                parameter
            )
            return Pair.of(
                selector,
                Expressions.call(
                    if (fieldComparator == null) BuiltInMethod.NULLS_COMPARATOR.method else BuiltInMethod.NULLS_COMPARATOR2.method,
                    Expressions.list(
                        Expressions.constant(
                            collation.nullDirection
                                    === RelFieldCollation.NullDirection.FIRST
                        ) as Expression,
                        Expressions.constant(
                            collation.direction
                                    === RelFieldCollation.Direction.DESCENDING
                        )
                    )
                        .appendIfNotNull(fieldComparator)
                )
            )
        }
        selector = Expressions.call(BuiltInMethod.IDENTITY_SELECTOR.method)

        // int c;
        // c = Utilities.compare(v0, v1);
        // if (c != 0) return c; // or -c if descending
        // ...
        // return 0;
        val body = BlockBuilder()
        val parameterV0: ParameterExpression = Expressions.parameter(javaRowClass, "v0")
        val parameterV1: ParameterExpression = Expressions.parameter(javaRowClass, "v1")
        val parameterC: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, "c")
        val mod = if (collations.size() === 1) Modifier.FINAL else 0
        body.add(Expressions.declare(mod, parameterC, null))
        for (collation in collations) {
            val index: Int = collation.getFieldIndex()
            val fieldType: RelDataType = rowType.getFieldList().get(index).getType()
            val fieldComparator: Expression = generateCollatorExpression(fieldType.getCollation())
            var arg0: Expression = fieldReference(parameterV0, index)
            var arg1: Expression = fieldReference(parameterV1, index)
            when (Primitive.flavor(fieldClass(index))) {
                OBJECT -> {
                    arg0 = EnumUtils.convert(arg0, Comparable::class.java)
                    arg1 = EnumUtils.convert(arg1, Comparable::class.java)
                }
                else -> {}
            }
            val nullsFirst = (collation.nullDirection
                    === RelFieldCollation.NullDirection.FIRST)
            val descending = (collation.getDirection()
                    === RelFieldCollation.Direction.DESCENDING)
            body.add(
                Expressions.statement(
                    Expressions.assign(
                        parameterC,
                        Expressions.call(
                            Utilities::class.java,
                            if (fieldNullable(index)) if (nullsFirst != descending) "compareNullsFirst" else "compareNullsLast" else "compare",
                            Expressions.list(
                                arg0,
                                arg1
                            )
                                .appendIfNotNull(fieldComparator)
                        )
                    )
                )
            )
            body.add(
                Expressions.ifThen(
                    Expressions.notEqual(
                        parameterC, Expressions.constant(0)
                    ),
                    Expressions.return_(
                        null,
                        if (descending) Expressions.negate(parameterC) else parameterC
                    )
                )
            )
        }
        body.add(
            Expressions.return_(null, Expressions.constant(0))
        )
        val memberDeclarations: List<MemberDeclaration> = Expressions.list(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                Int::class.javaPrimitiveType,
                "compare",
                ImmutableList.of(
                    parameterV0, parameterV1
                ),
                body.toBlock()
            )
        )
        if (EnumerableRules.BRIDGE_METHODS) {
            val parameterO0: ParameterExpression = Expressions.parameter(Object::class.java, "o0")
            val parameterO1: ParameterExpression = Expressions.parameter(Object::class.java, "o1")
            val bridgeBody = BlockBuilder()
            bridgeBody.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        Expressions.parameter(
                            Comparable::class.java, "this"
                        ),
                        BuiltInMethod.COMPARATOR_COMPARE.method,
                        Expressions.convert_(
                            parameterO0,
                            javaRowClass
                        ),
                        Expressions.convert_(
                            parameterO1,
                            javaRowClass
                        )
                    )
                )
            )
            memberDeclarations.add(
                overridingMethodDecl(
                    BuiltInMethod.COMPARATOR_COMPARE.method,
                    ImmutableList.of(parameterO0, parameterO1),
                    bridgeBody.toBlock()
                )
            )
        }
        return Pair.of(
            selector,
            Expressions.new_(
                Comparator::class.java,
                ImmutableList.of(),
                memberDeclarations
            )
        )
    }

    @Override
    override fun generateComparator(collation: RelCollation): Expression {
        // int c;
        // c = Utilities.compare(v0, v1);
        // if (c != 0) return c; // or -c if descending
        // ...
        // return 0;
        val body = BlockBuilder()
        val javaRowClass: Type = Primitive.box(javaRowClass)
        val parameterV0: ParameterExpression = Expressions.parameter(javaRowClass, "v0")
        val parameterV1: ParameterExpression = Expressions.parameter(javaRowClass, "v1")
        val parameterC: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, "c")
        val mod = if (collation.getFieldCollations().size() === 1) Modifier.FINAL else 0
        body.add(Expressions.declare(mod, parameterC, null))
        for (fieldCollation in collation.getFieldCollations()) {
            val index: Int = fieldCollation.getFieldIndex()
            val fieldType: RelDataType = rowType.getFieldList().get(index).getType()
            val fieldComparator: Expression = generateCollatorExpression(fieldType.getCollation())
            var arg0: Expression = fieldReference(parameterV0, index)
            var arg1: Expression = fieldReference(parameterV1, index)
            when (Primitive.flavor(fieldClass(index))) {
                OBJECT -> {
                    arg0 = EnumUtils.convert(arg0, Comparable::class.java)
                    arg1 = EnumUtils.convert(arg1, Comparable::class.java)
                }
                else -> {}
            }
            val nullsFirst = (fieldCollation.nullDirection
                    === RelFieldCollation.NullDirection.FIRST)
            val descending = (fieldCollation.getDirection()
                    === RelFieldCollation.Direction.DESCENDING)
            body.add(
                Expressions.statement(
                    Expressions.assign(
                        parameterC,
                        Expressions.call(
                            Utilities::class.java,
                            if (fieldNullable(index)) if (nullsFirst != descending) "compareNullsFirst" else "compareNullsLast" else "compare",
                            Expressions.list(
                                arg0,
                                arg1
                            )
                                .appendIfNotNull(fieldComparator)
                        )
                    )
                )
            )
            body.add(
                Expressions.ifThen(
                    Expressions.notEqual(
                        parameterC, Expressions.constant(0)
                    ),
                    Expressions.return_(
                        null,
                        if (descending) Expressions.negate(parameterC) else parameterC
                    )
                )
            )
        }
        body.add(
            Expressions.return_(null, Expressions.constant(0))
        )
        val memberDeclarations: List<MemberDeclaration> = Expressions.list(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                Int::class.javaPrimitiveType,
                "compare",
                ImmutableList.of(parameterV0, parameterV1),
                body.toBlock()
            )
        )
        if (EnumerableRules.BRIDGE_METHODS) {
            val parameterO0: ParameterExpression = Expressions.parameter(Object::class.java, "o0")
            val parameterO1: ParameterExpression = Expressions.parameter(Object::class.java, "o1")
            val bridgeBody = BlockBuilder()
            bridgeBody.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        Expressions.parameter(
                            Comparable::class.java, "this"
                        ),
                        BuiltInMethod.COMPARATOR_COMPARE.method,
                        Expressions.convert_(
                            parameterO0,
                            javaRowClass
                        ),
                        Expressions.convert_(
                            parameterO1,
                            javaRowClass
                        )
                    )
                )
            )
            memberDeclarations.add(
                overridingMethodDecl(
                    BuiltInMethod.COMPARATOR_COMPARE.method,
                    ImmutableList.of(parameterO0, parameterO1),
                    bridgeBody.toBlock()
                )
            )
        }
        return Expressions.new_(
            Comparator::class.java,
            ImmutableList.of(),
            memberDeclarations
        )
    }

    @Override
    fun getRowType(): RelDataType {
        return rowType
    }

    @Override
    override fun record(expressions: List<Expression?>?): Expression {
        return format.record(javaRowClass, expressions)
    }

    @get:Override
    override val javaRowType: Type
        get() = javaRowClass

    @Override
    override fun getJavaFieldType(index: Int): Type {
        return format.javaFieldClass(typeFactory, rowType, index)
    }

    @Override
    override fun component(fieldOrdinal: Int): PhysType {
        val field: RelDataTypeField = rowType.getFieldList().get(fieldOrdinal)
        val componentType: RelDataType = requireNonNull(
            field.getType().getComponentType()
        ) { "field.getType().getComponentType() for $field" }
        return of(
            typeFactory,
            toStruct(componentType), format, false
        )
    }

    @Override
    override fun field(ordinal: Int): PhysType {
        val field: RelDataTypeField = rowType.getFieldList().get(ordinal)
        val type: RelDataType = field.getType()
        return of(typeFactory, toStruct(type), format, false)
    }

    private fun toStruct(type: RelDataType): RelDataType {
        return if (type.isStruct()) {
            type
        } else typeFactory.builder()
            .add(SqlUtil.deriveAliasFromOrdinal(0), type)
            .build()
    }

    @Override
    @Nullable
    override fun comparer(): Expression? {
        return format.comparer()
    }

    private fun fieldReferences(
        parameter: Expression?, fields: List<Integer>
    ): List<Expression> {
        return object : AbstractList<Expression?>() {
            @Override
            operator fun get(index: Int): Expression {
                return fieldReference(parameter, fields[index])
            }

            @Override
            fun size(): Int {
                return fields.size()
            }
        }
    }

    @Override
    override fun fieldClass(field: Int): Class {
        return fieldClasses[field]
    }

    @Override
    override fun fieldNullable(field: Int): Boolean {
        return rowType.getFieldList().get(field).getType().isNullable()
    }

    @Override
    fun generateAccessor(
        fields: List<Integer>
    ): Expression {
        val v1: ParameterExpression = Expressions.parameter(javaRowClass, "v1")
        return when (fields.size()) {
            0 -> Expressions.lambda(
                Function1::class.java,
                Expressions.field(
                    null,
                    BuiltInMethod.COMPARABLE_EMPTY_LIST.field
                ),
                v1
            )
            1 -> {
                val field0: Int = fields[0]

                // new Function1<Employee, Res> {
                //    public Res apply(Employee v1) {
                //        return v1.<fieldN>;
                //    }
                // }
                val returnType: Class = fieldClasses[field0]
                val fieldReference: Expression = EnumUtils.convert(
                    fieldReference(v1, field0),
                    returnType
                )
                Expressions.lambda(
                    Function1::class.java,
                    fieldReference,
                    v1
                )
            }
            else -> {
                // new Function1<Employee, List> {
                //    public List apply(Employee v1) {
                //        return Arrays.asList(
                //            new Object[] {v1.<fieldN>, v1.<fieldM>});
                //    }
                // }
                val list: Expressions.FluentList<Expression> = Expressions.list()
                for (field in fields) {
                    list.add(fieldReference(v1, field))
                }
                when (list.size()) {
                    2 -> Expressions.lambda(
                        Function1::class.java,
                        Expressions.call(
                            List::class.java,
                            null,
                            BuiltInMethod.LIST2.method,
                            list
                        ),
                        v1
                    )
                    3 -> Expressions.lambda(
                        Function1::class.java,
                        Expressions.call(
                            List::class.java,
                            null,
                            BuiltInMethod.LIST3.method,
                            list
                        ),
                        v1
                    )
                    4 -> Expressions.lambda(
                        Function1::class.java,
                        Expressions.call(
                            List::class.java,
                            null,
                            BuiltInMethod.LIST4.method,
                            list
                        ),
                        v1
                    )
                    5 -> Expressions.lambda(
                        Function1::class.java,
                        Expressions.call(
                            List::class.java,
                            null,
                            BuiltInMethod.LIST5.method,
                            list
                        ),
                        v1
                    )
                    6 -> Expressions.lambda(
                        Function1::class.java,
                        Expressions.call(
                            List::class.java,
                            null,
                            BuiltInMethod.LIST6.method,
                            list
                        ),
                        v1
                    )
                    else -> Expressions.lambda(
                        Function1::class.java,
                        Expressions.call(
                            List::class.java,
                            null,
                            BuiltInMethod.LIST_N.method,
                            Expressions.newArrayInit(
                                Comparable::class.java,
                                list
                            )
                        ),
                        v1
                    )
                }
            }
        }
    }

    @Override
    override fun fieldReference(
        expression: Expression?, field: Int
    ): Expression {
        return fieldReference(expression, field, null)
    }

    @Override
    override fun fieldReference(
        expression: Expression?, field: Int, @Nullable storageType: Type?
    ): Expression {
        var storageType: Type? = storageType
        var fieldType: Type?
        if (storageType == null) {
            storageType = fieldClass(field)
            fieldType = null
        } else {
            fieldType = fieldClass(field)
            if (fieldType !== java.sql.Date::class.java && fieldType !== Time::class.java && fieldType !== java.sql.Timestamp::class.java) {
                fieldType = null
            }
        }
        return format.field(expression, field, fieldType, storageType)
    }

    companion object {
        fun of(
            typeFactory: JavaTypeFactory,
            rowType: RelDataType,
            format: JavaRowFormat
        ): PhysType {
            return of(typeFactory, rowType, format, true)
        }

        fun of(
            typeFactory: JavaTypeFactory,
            rowType: RelDataType,
            format: JavaRowFormat,
            optimize: Boolean
        ): PhysType {
            var format: JavaRowFormat = format
            if (optimize) {
                format = format.optimize(rowType)
            }
            val javaRowClass: Type = format.javaRowClass(typeFactory, rowType)
            return PhysTypeImpl(typeFactory, rowType, javaRowClass, format)
        }

        fun of(
            typeFactory: JavaTypeFactory,
            javaRowClass: Type
        ): PhysType {
            val builder: RelDataTypeFactory.Builder = typeFactory.builder()
            if (javaRowClass is Types.RecordType) {
                val recordType: Types.RecordType = javaRowClass as Types.RecordType
                for (field in recordType.getRecordFields()) {
                    builder.add(field.getName(), typeFactory.createType(field.getType()))
                }
            }
            val rowType: RelDataType = builder.build()
            // Do not optimize if there are 0 or 1 fields.
            return PhysTypeImpl(
                typeFactory, rowType, javaRowClass,
                JavaRowFormat.CUSTOM
            )
        }
    }
}
