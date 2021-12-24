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
package org.apache.calcite.rel.mutable

import org.apache.calcite.plan.RelOptTable

/** Mutable equivalent of [org.apache.calcite.rel.core.TableScan].  */
class MutableScan private constructor(rel: TableScan?) : MutableLeafRel(MutableRelType.TABLE_SCAN, rel) {
    @Nullable
    private fun tableQualifiedName(): List<String>? {
        val table: RelOptTable = rel.getTable()
        return if (table == null) null else table.getQualifiedName()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (obj !is MutableScan) {
            return false
        }
        val other = obj as MutableScan
        return (obj === this
                || Objects.equals(tableQualifiedName(), other.tableQualifiedName()))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hashCode(tableQualifiedName())
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Scan(table: ")
            .append(tableQualifiedName()).append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(rel as TableScan?)
    }

    companion object {
        /**
         * Creates a MutableScan.
         *
         * @param scan  The underlying TableScan object
         */
        fun of(scan: TableScan?): MutableScan {
            return MutableScan(scan)
        }
    }
}
