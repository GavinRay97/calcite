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

import org.apache.calcite.linq4j.tree.Expression

/**
 * Physical type of a row.
 *
 *
 * Consists of the SQL row type (returned by [.getRowType]), the Java
 * type of the row (returned by [.getJavaRowType]), and methods to
 * generate expressions to access fields, generate records, and so forth.
 * Together, the records encapsulate how the logical type maps onto the physical
 * type.
 */
interface PhysType {
    /** Returns the Java type (often a Class) that represents a row. For
     * example, in one row format, always returns `Object[].class`.  */
    val javaRowType: Type

    /**
     * Returns the Java class that is used to store the field with the given
     * ordinal.
     *
     *
     * For instance, when the java row type is `Object[]`, the java
     * field type is `Object` even if the field is not nullable.  */
    fun getJavaFieldType(field: Int): Type?

    /** Returns the physical type of a field.  */
    fun field(ordinal: Int): PhysType?

    /** Returns the physical type of a given field's component type.  */
    fun component(field: Int): PhysType?

    /** Returns the SQL row type.  */
    val rowType: RelDataType

    /** Returns the Java class of the field with the given ordinal.  */
    fun fieldClass(field: Int): Class?

    /** Returns whether a given field allows null values.  */
    fun fieldNullable(index: Int): Boolean

    /** Generates a reference to a given field in an expression.
     *
     *
     * For example given `expression=employee` and `field=2`,
     * generates
     *
     * <blockquote><pre>`employee.deptno`</pre></blockquote>
     *
     * @param expression Expression
     * @param field Ordinal of field
     * @return Expression to access the field of the expression
     */
    fun fieldReference(expression: Expression?, field: Int): Expression?

    /** Generates a reference to a given field in an expression.
     *
     *
     * This method optimizes for the target storage type (i.e. avoids
     * casts).
     *
     *
     * For example given `expression=employee` and `field=2`,
     * generates
     *
     * <blockquote><pre>`employee.deptno`</pre></blockquote>
     *
     * @param expression Expression
     * @param field Ordinal of field
     * @param storageType optional hint for storage class
     * @return Expression to access the field of the expression
     */
    fun fieldReference(
        expression: Expression?, field: Int,
        @Nullable storageType: Type?
    ): Expression

    /** Generates an accessor function for a given list of fields.  The resulting
     * object is a [List] (implementing [Object.hashCode] and
     * [Object.equals] per that interface) and also implements
     * [Comparable].
     *
     *
     * For example:
     *
     * <blockquote><pre>
     * new Function1&lt;Employee, Object[]&gt; {
     * public Object[] apply(Employee v1) {
     * return FlatLists.of(v1.&lt;fieldN&gt;, v1.&lt;fieldM&gt;);
     * }
     * }
     * }</pre></blockquote>
     */
    fun generateAccessor(fields: List<Integer?>?): Expression?

    /** Generates a selector for the given fields from an expression, with the
     * default row format.  */
    fun generateSelector(
        parameter: ParameterExpression?,
        fields: List<Integer?>?
    ): Expression?

    /** Generates a lambda expression that is a selector for the given fields from
     * an expression.  */
    fun generateSelector(
        parameter: ParameterExpression?,
        fields: List<Integer?>?,
        targetFormat: JavaRowFormat?
    ): Expression?

    /** Generates a lambda expression that is a selector for the given fields from
     * an expression.
     *
     *
     * `usedFields` must be a subset of `fields`.
     * For each field, there is a corresponding indicator field.
     * If a field is used, its value is assigned and its indicator is left
     * `false`.
     * If a field is not used, its value is not assigned and its indicator is
     * set to `true`;
     * This will become a value of 1 when `GROUPING(field)` is called.  */
    fun generateSelector(
        parameter: ParameterExpression?,
        fields: List<Integer?>?,
        usedFields: List<Integer?>?,
        targetFormat: JavaRowFormat?
    ): Expression?

    /** Generates a selector for the given fields from an expression.
     * Only used by EnumerableWindow.  */
    fun selector(
        parameter: ParameterExpression?,
        fields: List<Integer?>?,
        targetFormat: JavaRowFormat?
    ): Pair<Type?, List<Expression?>?>?

    /** Projects a given collection of fields from this input record, into
     * a particular preferred output format. The output format is optimized
     * if there are 0 or 1 fields.  */
    fun project(
        integers: List<Integer?>?,
        format: JavaRowFormat?
    ): PhysType?

    /** Projects a given collection of fields from this input record, optionally
     * with indicator fields, into a particular preferred output format.
     *
     *
     * The output format is optimized if there are 0 or 1 fields
     * and indicators are disabled.  */
    fun project(
        integers: List<Integer?>?,
        indicator: Boolean,
        format: JavaRowFormat?
    ): PhysType?

    /** Returns a lambda to create a collation key and a comparator. The
     * comparator is sometimes null.  */
    fun generateCollationKey(
        collations: List<RelFieldCollation?>?
    ): Pair<Expression?, Expression?>?

    /** Returns a comparator. Unlike the comparator returned by
     * [.generateCollationKey], this comparator acts on the
     * whole element.  */
    fun generateComparator(
        collation: RelCollation?
    ): Expression?

    /** Returns a expression that yields a comparer, or null if this type
     * is comparable.  */
    @Nullable
    fun comparer(): Expression?

    /** Generates an expression that creates a record for a row, initializing
     * its fields with the given expressions. There must be one expression per
     * field.
     *
     * @param expressions Expression to initialize each field
     * @return Expression to create a row
     */
    fun record(expressions: List<Expression?>?): Expression?

    /** Returns the format.  */
    val format: org.apache.calcite.adapter.enumerable.JavaRowFormat
    fun accessors(parameter: Expression?, argList: List<Integer?>?): List<Expression?>?

    /** Returns a copy of this type that allows nulls if `nullable` is
     * true.  */
    fun makeNullable(nullable: Boolean): PhysType?

    /** Converts an enumerable of this physical type to an enumerable that uses a
     * given physical type for its rows.
     *
     */
    @Deprecated
    @Deprecated(
        """Use {@link #convertTo(Expression, JavaRowFormat)}.
    The use of PhysType as a second parameter is misleading since only the row
    format of the expression is affected by the conversion. Moreover it requires
    to have at hand a PhysType object which is not really necessary for achieving
    the desired result. """
    )
    fun  // to be removed before 2.0
            convertTo(expression: Expression?, targetPhysType: PhysType?): Expression?

    /** Converts an enumerable of this physical type to an enumerable that uses
     * the `targetFormat` for representing its rows.  */
    fun convertTo(expression: Expression?, targetFormat: JavaRowFormat?): Expression
}
