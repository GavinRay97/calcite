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
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.linq4j.function.Function1
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.runtime.Enumerables
import org.apache.calcite.schema.FilterableTable
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.calcite.schema.QueryableTable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Schemas
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.Util
import org.apache.calcite.util.mapping.Mapping
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.lang.reflect.Type
import java.util.List
import org.apache.calcite.util.Static.RESOURCE
import java.util.Objects.requireNonNull

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.TableScan].
 */
class TableScanNode private constructor(
    compiler: Compiler, rel: TableScan,
    enumerable: Enumerable<Row>?
) : Node {
    init {
        compiler.enumerable(rel, enumerable)
    }

    @Override
    fun run() {
        // nothing to do
    }

    companion object {
        /** Creates a TableScanNode.
         *
         *
         * Tries various table SPIs, and negotiates with the table which filters
         * and projects it can implement. Adds to the Enumerable implementations of
         * any filters and projects that cannot be implemented by the table.  */
        fun create(
            compiler: Compiler, rel: TableScan,
            filters: ImmutableList<RexNode>, @Nullable projects: ImmutableIntList
        ): TableScanNode {
            val relOptTable: RelOptTable = rel.getTable()
            val pfTable: ProjectableFilterableTable = relOptTable.unwrap(ProjectableFilterableTable::class.java)
            if (pfTable != null) {
                return createProjectableFilterable(
                    compiler, rel, filters, projects,
                    pfTable
                )
            }
            val filterableTable: FilterableTable = relOptTable.unwrap(FilterableTable::class.java)
            if (filterableTable != null) {
                return createFilterable(
                    compiler, rel, filters, projects,
                    filterableTable
                )
            }
            val scannableTable: ScannableTable = relOptTable.unwrap(ScannableTable::class.java)
            if (scannableTable != null) {
                return createScannable(
                    compiler, rel, filters, projects,
                    scannableTable
                )
            }
            val enumerable: Enumerable<Row> = relOptTable.unwrap(Enumerable::class.java)
            if (enumerable != null) {
                return createEnumerable(
                    compiler, rel, enumerable, null, filters,
                    projects
                )
            }
            val queryableTable: QueryableTable = relOptTable.unwrap(QueryableTable::class.java)
            if (queryableTable != null) {
                return createQueryable(
                    compiler, rel, filters, projects,
                    queryableTable
                )
            }
            throw AssertionError(
                "cannot convert table " + relOptTable
                        + " to enumerable"
            )
        }

        private fun createScannable(
            compiler: Compiler, rel: TableScan,
            filters: ImmutableList<RexNode>, @Nullable projects: ImmutableIntList,
            scannableTable: ScannableTable?
        ): TableScanNode {
            val rowEnumerable: Enumerable<Row> = Enumerables.toRow(scannableTable.scan(compiler.getDataContext()))
            return createEnumerable(
                compiler, rel, rowEnumerable, null, filters,
                projects
            )
        }

        private fun createQueryable(
            compiler: Compiler,
            rel: TableScan, filters: ImmutableList<RexNode>, @Nullable projects: ImmutableIntList,
            queryableTable: QueryableTable?
        ): TableScanNode {
            val root: DataContext = compiler.getDataContext()
            val relOptTable: RelOptTable = rel.getTable()
            val elementType: Type = queryableTable.getElementType()
            var schema: SchemaPlus = root.getRootSchema()
            for (name in Util.skipLast(relOptTable.getQualifiedName())) {
                requireNonNull(schema) {
                    ("schema is null while resolving " + name + " for table"
                            + relOptTable.getQualifiedName())
                }
                schema = schema.getSubSchema(name)
            }
            val rowEnumerable: Enumerable<Row>
            if (elementType is Class) {
                val queryable: Queryable<Object> = Schemas.queryable(
                    root,
                    elementType as Class,
                    relOptTable.getQualifiedName()
                )
                val fieldBuilder: ImmutableList.Builder<Field> = ImmutableList.builder()
                val type: Class = elementType as Class
                for (field in type.getFields()) {
                    if (Modifier.isPublic(field.getModifiers())
                        && !Modifier.isStatic(field.getModifiers())
                    ) {
                        fieldBuilder.add(field)
                    }
                }
                val fields: List<Field> = fieldBuilder.build()
                rowEnumerable = queryable.select { o ->
                    @Nullable val values: Array<Object?> = arrayOfNulls<Object>(fields.size())
                    for (i in 0 until fields.size()) {
                        val field: Field = fields[i]
                        try {
                            values[i] = field.get(o)
                        } catch (e: IllegalAccessException) {
                            throw RuntimeException(e)
                        }
                    }
                    Row(values)
                }
            } else {
                rowEnumerable = Schemas.queryable(root, Row::class.java, relOptTable.getQualifiedName())
            }
            return createEnumerable(
                compiler, rel, rowEnumerable, null, filters,
                projects
            )
        }

        private fun createFilterable(
            compiler: Compiler,
            rel: TableScan, filters: ImmutableList<RexNode>, @Nullable projects: ImmutableIntList,
            filterableTable: FilterableTable?
        ): TableScanNode {
            val root: DataContext = compiler.getDataContext()
            val mutableFilters: List<RexNode> = Lists.newArrayList(filters)
            val enumerable: Enumerable<Array<Object>> = filterableTable.scan(root, mutableFilters)
            for (filter in mutableFilters) {
                if (!filters.contains(filter)) {
                    throw RESOURCE.filterableTableInventedFilter(filter.toString()).ex()
                }
            }
            val rowEnumerable: Enumerable<Row> = Enumerables.toRow(enumerable)
            return createEnumerable(
                compiler, rel, rowEnumerable, null,
                mutableFilters, projects
            )
        }

        private fun createProjectableFilterable(
            compiler: Compiler,
            rel: TableScan, filters: ImmutableList<RexNode>, @Nullable projects: ImmutableIntList,
            pfTable: ProjectableFilterableTable
        ): TableScanNode {
            var projects: ImmutableIntList? = projects
            val root: DataContext = compiler.getDataContext()
            val originalProjects: ImmutableIntList? = projects
            while (true) {
                val mutableFilters: List<RexNode> = Lists.newArrayList(filters)
                val projectInts: IntArray?
                projectInts = if (projects == null
                    || projects.equals(TableScan.identity(rel.getTable()))
                ) {
                    null
                } else {
                    projects.toIntArray()
                }
                val enumerable1: Enumerable<Array<Object>> = pfTable.scan(root, mutableFilters, projectInts)
                for (filter in mutableFilters) {
                    if (!filters.contains(filter)) {
                        throw RESOURCE.filterableTableInventedFilter(filter.toString())
                            .ex()
                    }
                }
                val usedFields: ImmutableBitSet = RelOptUtil.InputFinder.bits(mutableFilters, null)
                if (projects != null) {
                    var changeCount = 0
                    for (usedField in usedFields) {
                        if (!projects.contains(usedField)) {
                            // A field that is not projected is used in a filter that the
                            // table rejected. We won't be able to apply the filter later.
                            // Try again without any projects.
                            projects = ImmutableIntList.copyOf(
                                Iterables.concat(projects, ImmutableList.of(usedField))
                            )
                            ++changeCount
                        }
                    }
                    if (changeCount > 0) {
                        continue
                    }
                }
                val rowEnumerable: Enumerable<Row> = Enumerables.toRow(enumerable1)
                val rejectedProjects: ImmutableIntList?
                rejectedProjects = if (originalProjects == null || originalProjects.equals(projects)) {
                    null
                } else {
                    // We projected extra columns because they were needed in filters. Now
                    // project the leading columns.
                    ImmutableIntList.identity(originalProjects.size())
                }
                return createEnumerable(
                    compiler, rel, rowEnumerable, projects,
                    mutableFilters, rejectedProjects
                )
            }
        }

        private fun createEnumerable(
            compiler: Compiler,
            rel: TableScan, enumerable: Enumerable<Row>?,
            @Nullable acceptedProjects: ImmutableIntList?, rejectedFilters: List<RexNode>,
            @Nullable rejectedProjects: ImmutableIntList?
        ): TableScanNode {
            var enumerable: Enumerable<Row>? = enumerable
            if (!rejectedFilters.isEmpty()) {
                val filter: RexNode = RexUtil.composeConjunction(
                    rel.getCluster().getRexBuilder(),
                    rejectedFilters
                )
                // Re-map filter for the projects that have been applied already
                val filter2: RexNode
                val inputRowType: RelDataType
                if (acceptedProjects == null) {
                    filter2 = filter
                    inputRowType = rel.getRowType()
                } else {
                    val mapping: Mapping = Mappings.target(
                        acceptedProjects,
                        rel.getTable().getRowType().getFieldCount()
                    )
                    filter2 = RexUtil.apply(mapping, filter)
                    val builder: RelDataTypeFactory.Builder = rel.getCluster().getTypeFactory().builder()
                    val fieldList: List<RelDataTypeField> = rel.getTable().getRowType().getFieldList()
                    for (acceptedProject in acceptedProjects) {
                        builder.add(fieldList[acceptedProject])
                    }
                    inputRowType = builder.build()
                }
                val condition: Scalar = compiler.compile(ImmutableList.of(filter2), inputRowType)
                val context: Context = compiler.createContext()
                enumerable = enumerable.where { row ->
                    context.values = row.getValues()
                    val b = condition.execute(context) as Boolean
                    b != null && b
                }
            }
            if (rejectedProjects != null) {
                enumerable = enumerable.select(
                    object : Function1<Row?, Row?>() {
                        @Nullable
                        val values: Array<Object?> = arrayOfNulls<Object>(rejectedProjects.size())
                        @Override
                        fun apply(row: Row): Row {
                            @Nullable val inValues: Array<Object> = row.getValues()
                            for (i in 0 until rejectedProjects.size()) {
                                values[i] = inValues[rejectedProjects.get(i)]
                            }
                            return Row.asCopy(values)
                        }
                    })
            }
            return TableScanNode(compiler, rel, enumerable)
        }
    }
}
