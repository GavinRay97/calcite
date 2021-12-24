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
package org.apache.calcite.rel.metadata

import org.apache.calcite.avatica.util.ByteString

/**
 * Default implementations of the
 * [org.apache.calcite.rel.metadata.BuiltInMetadata.Size]
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery.getAverageRowSize
 *
 * @see RelMetadataQuery.getAverageColumnSizes
 *
 * @see RelMetadataQuery.getAverageColumnSizesNotNull
 */
class RelMdSize  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<Size?> {
    //~ Methods ----------------------------------------------------------------
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.Size.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.Size.averageRowSize],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getAverageRowSize
     */
    @Nullable
    fun averageRowSize(rel: RelNode, mq: RelMetadataQuery): Double? {
        val averageColumnSizes: List<Double> = mq.getAverageColumnSizes(rel) ?: return null
        var d = 0.0
        val fields: List<RelDataTypeField> = rel.getRowType().getFieldList()
        for (p in Pair.zip(averageColumnSizes, fields)) {
            if (p.left == null) {
                val fieldValueSize = averageFieldValueSize(p.right) ?: return null
                d += fieldValueSize
            } else {
                d += p.left
            }
        }
        return d
    }

    /** Catch-all implementation for
     * [BuiltInMetadata.Size.averageColumnSizes],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getAverageColumnSizes
     */
    @Nullable
    fun averageColumnSizes(rel: RelNode?, mq: RelMetadataQuery?): List<Double>? {
        return null // absolutely no idea
    }

    @Nullable
    fun averageColumnSizes(rel: Filter, mq: RelMetadataQuery): List<Double> {
        return mq.getAverageColumnSizes(rel.getInput())
    }

    @Nullable
    fun averageColumnSizes(rel: Sort, mq: RelMetadataQuery): List<Double> {
        return mq.getAverageColumnSizes(rel.getInput())
    }

    @Nullable
    fun averageColumnSizes(rel: TableModify, mq: RelMetadataQuery): List<Double> {
        return mq.getAverageColumnSizes(rel.getInput())
    }

    @Nullable
    fun averageColumnSizes(rel: Exchange, mq: RelMetadataQuery): List<Double> {
        return mq.getAverageColumnSizes(rel.getInput())
    }

    @Nullable
    fun averageColumnSizes(rel: Project, mq: RelMetadataQuery): List<Double> {
        val inputColumnSizes: List<Double> = mq.getAverageColumnSizesNotNull(rel.getInput())
        val sizes: ImmutableNullableList.Builder<Double> = ImmutableNullableList.builder()
        for (project in rel.getProjects()) {
            sizes.add(averageRexSize(project, inputColumnSizes))
        }
        return sizes.build()
    }

    @Nullable
    fun averageColumnSizes(rel: Calc, mq: RelMetadataQuery): List<Double> {
        val inputColumnSizes: List<Double> = mq.getAverageColumnSizesNotNull(rel.getInput())
        val sizes: ImmutableNullableList.Builder<Double> = ImmutableNullableList.builder()
        rel.getProgram().split().left.forEach { exp -> sizes.add(averageRexSize(exp, inputColumnSizes)) }
        return sizes.build()
    }

    @Nullable
    fun averageColumnSizes(rel: Values, mq: RelMetadataQuery?): List<Double> {
        val fields: List<RelDataTypeField> = rel.getRowType().getFieldList()
        val list: ImmutableNullableList.Builder<Double> = ImmutableNullableList.builder()
        for (i in 0 until fields.size()) {
            val field: RelDataTypeField = fields[i]
            if (rel.getTuples().isEmpty()) {
                list.add(averageTypeValueSize(field.getType()))
            } else {
                var d = 0.0
                for (literals in rel.getTuples()) {
                    d += typeValueSize(
                        field.getType(),
                        literals.get(i).getValueAs(Comparable::class.java)
                    )
                }
                d /= rel.getTuples().size()
                list.add(d)
            }
        }
        return list.build()
    }

    fun averageColumnSizes(rel: TableScan, mq: RelMetadataQuery?): List<Double> {
        val fields: List<RelDataTypeField> = rel.getRowType().getFieldList()
        val list: ImmutableNullableList.Builder<Double> = ImmutableNullableList.builder()
        for (field in fields) {
            list.add(averageTypeValueSize(field.getType()))
        }
        return list.build()
    }

    fun averageColumnSizes(rel: Aggregate, mq: RelMetadataQuery): List<Double> {
        val inputColumnSizes: List<Double> = mq.getAverageColumnSizesNotNull(rel.getInput())
        val list: ImmutableNullableList.Builder<Double> = ImmutableNullableList.builder()
        for (key in rel.getGroupSet()) {
            list.add(inputColumnSizes[key])
        }
        for (aggregateCall in rel.getAggCallList()) {
            list.add(averageTypeValueSize(aggregateCall.type))
        }
        return list.build()
    }

    @Nullable
    fun averageColumnSizes(rel: Join, mq: RelMetadataQuery): List<Double>? {
        return averageJoinColumnSizes(rel, mq)
    }

    @Nullable
    fun averageColumnSizes(rel: Intersect, mq: RelMetadataQuery): List<Double> {
        return mq.getAverageColumnSizes(rel.getInput(0))
    }

    @Nullable
    fun averageColumnSizes(rel: Minus, mq: RelMetadataQuery): List<Double> {
        return mq.getAverageColumnSizes(rel.getInput(0))
    }

    @Nullable
    fun averageColumnSizes(rel: Union, mq: RelMetadataQuery): List<Double>? {
        val fieldCount: Int = rel.getRowType().getFieldCount()
        val inputColumnSizeList: List<List<Double>> = ArrayList()
        for (input in rel.getInputs()) {
            val inputSizes: List<Double> = mq.getAverageColumnSizes(input)
            if (inputSizes != null) {
                inputColumnSizeList.add(inputSizes)
            }
        }
        when (inputColumnSizeList.size()) {
            0 -> return null // all were null
            1 -> return inputColumnSizeList[0] // all but one were null
            else -> {}
        }
        val sizes: ImmutableNullableList.Builder<Double> = ImmutableNullableList.builder()
        var nn = 0
        for (i in 0 until fieldCount) {
            var d = 0.0
            var n = 0
            for (inputColumnSizes in inputColumnSizeList) {
                val d2 = inputColumnSizes[i]
                if (d2 != null) {
                    d += d2
                    ++n
                    ++nn
                }
            }
            sizes.add(if (n > 0) d / n else null)
        }
        return if (nn == 0) {
            null // all columns are null
        } else sizes.build()
    }

    /** Estimates the average size (in bytes) of a value of a field, knowing
     * nothing more than its type.
     *
     *
     * We assume that the proportion of nulls is negligible, even if the field
     * is nullable.
     */
    @Nullable
    protected fun averageFieldValueSize(field: RelDataTypeField): Double? {
        return averageTypeValueSize(field.getType())
    }

    /** Estimates the average size (in bytes) of a value of a type.
     *
     *
     * We assume that the proportion of nulls is negligible, even if the type
     * is nullable.
     */
    @Nullable
    fun averageTypeValueSize(type: RelDataType): Double? {
        return when (type.getSqlTypeName()) {
            BOOLEAN, TINYINT -> 1.0
            SMALLINT -> 2.0
            INTEGER, REAL, DECIMAL, DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> 4.0
            BIGINT, DOUBLE, FLOAT, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> 8.0
            BINARY -> type.getPrecision()
            VARBINARY -> Math.min(type.getPrecision() as Double, 100.0)
            CHAR -> type.getPrecision() as Double * BYTES_PER_CHARACTER
            VARCHAR ->       // Even in large (say VARCHAR(2000)) columns most strings are small
                Math.min(type.getPrecision() as Double * BYTES_PER_CHARACTER, 100.0)
            ROW -> {
                var average = 0.0
                for (field in type.getFieldList()) {
                    val size = averageTypeValueSize(field.getType())
                    if (size != null) {
                        average += size
                    }
                }
                average
            }
            else -> null
        }
    }

    /** Estimates the average size (in bytes) of a value of a type.
     *
     *
     * Nulls count as 1 byte.
     */
    fun typeValueSize(type: RelDataType, @Nullable value: Comparable?): Double {
        return if (value == null) {
            1.0
        } else when (type.getSqlTypeName()) {
            BOOLEAN, TINYINT -> 1.0
            SMALLINT -> 2.0
            INTEGER, FLOAT, REAL, DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> 4.0
            BIGINT, DOUBLE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> 8.0
            BINARY, VARBINARY -> (value as ByteString).length()
            CHAR, VARCHAR -> (value as NlsString).getValue()
                .length() * BYTES_PER_CHARACTER
            else -> 32
        }
    }

    @Nullable
    fun averageRexSize(
        node: RexNode,
        inputColumnSizes: List<Double?>
    ): Double? {
        return when (node.getKind()) {
            INPUT_REF -> inputColumnSizes[(node as RexInputRef).getIndex()]
            LITERAL -> typeValueSize(
                node.getType(),
                (node as RexLiteral).getValueAs(Comparable::class.java)
            )
            else -> {
                if (node is RexCall) {
                    val call: RexCall = node as RexCall
                    for (operand in call.getOperands()) {
                        // It's a reasonable assumption that a function's result will have
                        // similar size to its argument of a similar type. For example,
                        // UPPER(c) has the same average size as c.
                        if (operand.getType().getSqlTypeName()
                            === node.getType().getSqlTypeName()
                        ) {
                            return averageRexSize(operand, inputColumnSizes)
                        }
                    }
                }
                averageTypeValueSize(node.getType())
            }
        }
    }

    companion object {
        /** Source for
         * [org.apache.calcite.rel.metadata.BuiltInMetadata.Size].  */
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdSize(),
            BuiltInMetadata.Size.Handler::class.java
        )

        /** Bytes per character (2).  */
        val BYTES_PER_CHARACTER: Int = Character.SIZE / Byte.SIZE
        @Nullable
        private fun averageJoinColumnSizes(
            rel: Join,
            mq: RelMetadataQuery
        ): List<Double>? {
            val semiOrAntijoin: Boolean = !rel.getJoinType().projectsRight()
            val left: RelNode = rel.getLeft()
            val right: RelNode = rel.getRight()
            @Nullable val lefts: List<Double> = mq.getAverageColumnSizes(left)
            @Nullable val rights: List<Double>? = if (semiOrAntijoin) null else mq.getAverageColumnSizes(right)
            if (lefts == null && rights == null) {
                return null
            }
            val fieldCount: Int = rel.getRowType().getFieldCount()
            @Nullable val sizes = arrayOfNulls<Double>(fieldCount)
            if (lefts != null) {
                lefts.toArray(sizes)
            }
            if (rights != null) {
                val leftCount: Int = left.getRowType().getFieldCount()
                for (i in 0 until rights.size()) {
                    sizes[leftCount + i] = rights[i]
                }
            }
            return ImmutableNullableList.copyOf(sizes)
        }
    }
}
