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

import org.apache.calcite.plan.volcano.RelSubset

/**
 * Default implementation of [RelMetadataQuery.getTableReferences] for the
 * standard logical algebra.
 *
 *
 * The goal of this provider is to return all tables used by a given
 * expression identified uniquely by a [RelTableRef].
 *
 *
 * Each unique identifier [RelTableRef] of a table will equal to the
 * identifier obtained running [RelMdExpressionLineage] over the same plan
 * node for an expression that refers to the same table.
 *
 *
 * If tables cannot be obtained, we return null.
 */
class RelMdTableReferences  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<BuiltInMetadata.TableReferences?> {
    //~ Methods ----------------------------------------------------------------
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.TableReferences.DEF

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getTableReferences(rel: RelNode?, mq: RelMetadataQuery?): Set<RelTableRef>? {
        return null
    }

    @Nullable
    fun getTableReferences(rel: RelSubset, mq: RelMetadataQuery): Set<RelTableRef>? {
        val bestOrOriginal: RelNode = Util.first(rel.getBest(), rel.getOriginal()) ?: return null
        return mq.getTableReferences(bestOrOriginal)
    }

    /**
     * TableScan table reference.
     */
    fun getTableReferences(rel: TableScan, mq: RelMetadataQuery?): Set<RelTableRef> {
        return ImmutableSet.of(RelTableRef.of(rel.getTable(), 0))
    }

    /**
     * Table references from Aggregate.
     */
    @Nullable
    fun getTableReferences(rel: Aggregate, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Join.
     */
    @Nullable
    fun getTableReferences(rel: Join, mq: RelMetadataQuery): Set<RelTableRef>? {
        val leftInput: RelNode = rel.getLeft()
        val rightInput: RelNode = rel.getRight()
        val result: Set<RelTableRef> = HashSet()

        // Gather table references, left input references remain unchanged
        val leftQualifiedNamesToRefs: Multimap<List<String>, RelTableRef> = HashMultimap.create()
        val leftTableRefs: Set<RelTableRef> = mq.getTableReferences(leftInput)
            ?: // We could not infer the table refs from left input
            return null
        for (leftRef in leftTableRefs) {
            assert(!result.contains(leftRef))
            result.add(leftRef)
            leftQualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef)
        }

        // Gather table references, right input references might need to be
        // updated if there are table names clashes with left input
        val rightTableRefs: Set<RelTableRef> = mq.getTableReferences(rightInput)
            ?: // We could not infer the table refs from right input
            return null
        for (rightRef in rightTableRefs) {
            var shift = 0
            val lRefs: Collection<RelTableRef> = leftQualifiedNamesToRefs.get(rightRef.getQualifiedName())
            if (lRefs != null) {
                shift = lRefs.size()
            }
            val shiftTableRef: RelTableRef = RelTableRef.of(
                rightRef.getTable(), shift + rightRef.getEntityNumber()
            )
            assert(!result.contains(shiftTableRef))
            result.add(shiftTableRef)
        }

        // Return result
        return result
    }

    /**
     * Table references from Union, Intersect, Minus.
     *
     *
     * For Union operator, we might be able to extract multiple table
     * references.
     */
    @Nullable
    fun getTableReferences(rel: SetOp, mq: RelMetadataQuery): Set<RelTableRef>? {
        val result: Set<RelTableRef> = HashSet()

        // Infer column origin expressions for given references
        val qualifiedNamesToRefs: Multimap<List<String>, RelTableRef> = HashMultimap.create()
        for (input in rel.getInputs()) {
            val currentTablesMapping: Map<RelTableRef, RelTableRef> = HashMap()
            val inputTableRefs: Set<RelTableRef> = mq.getTableReferences(input)
                ?: // We could not infer the table refs from input
                return null
            for (tableRef in inputTableRefs) {
                var shift = 0
                val lRefs: Collection<RelTableRef> = qualifiedNamesToRefs.get(
                    tableRef.getQualifiedName()
                )
                if (lRefs != null) {
                    shift = lRefs.size()
                }
                val shiftTableRef: RelTableRef = RelTableRef.of(
                    tableRef.getTable(), shift + tableRef.getEntityNumber()
                )
                assert(!result.contains(shiftTableRef))
                result.add(shiftTableRef)
                currentTablesMapping.put(tableRef, shiftTableRef)
            }
            // Add to existing qualified names
            for (newRef in currentTablesMapping.values()) {
                qualifiedNamesToRefs.put(newRef.getQualifiedName(), newRef)
            }
        }

        // Return result
        return result
    }

    /**
     * Table references from Project.
     */
    @Nullable
    fun getTableReferences(rel: Project, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Filter.
     */
    @Nullable
    fun getTableReferences(rel: Filter, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Calc.
     */
    @Nullable
    fun getTableReferences(rel: Calc, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Sort.
     */
    @Nullable
    fun getTableReferences(rel: Sort, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from TableModify.
     */
    @Nullable
    fun getTableReferences(rel: TableModify, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Exchange.
     */
    @Nullable
    fun getTableReferences(rel: Exchange, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Window.
     */
    @Nullable
    fun getTableReferences(rel: Window, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    /**
     * Table references from Sample.
     */
    @Nullable
    fun getTableReferences(rel: Sample, mq: RelMetadataQuery): Set<RelTableRef> {
        return mq.getTableReferences(rel.getInput())
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdTableReferences(), BuiltInMetadata.TableReferences.Handler::class.java
        )
    }
}
