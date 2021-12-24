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
package org.apache.calcite.schema

import org.apache.calcite.plan.RelOptCluster

/**
 * A table that can be modified.
 *
 *
 * NOTE: The current API is inefficient and experimental. It will change
 * without notice.
 *
 * @see ModifiableView
 */
interface ModifiableTable : QueryableTable {
    /** Returns the modifiable collection.
     * Modifying the collection will change the table's contents.  */
    @get:Nullable
    val modifiableCollection: Collection?

    /** Creates a relational expression that modifies this table.  */
    fun toModificationRel(
        cluster: RelOptCluster?,
        table: RelOptTable?,
        catalogReader: CatalogReader?,
        child: RelNode?,
        operation: TableModify.Operation?,
        @Nullable updateColumnList: List<String?>?,
        @Nullable sourceExpressionList: List<RexNode?>?,
        flattened: Boolean
    ): TableModify?
}
