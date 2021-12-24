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

import org.apache.calcite.rel.type.RelDataTypeField

/**
 * Table whose row type can be extended to include extra fields.
 *
 *
 * In some storage systems, especially those with "late schema", there may
 * exist columns that have values in the table but which are not declared in
 * the table schema. However, a particular query may wish to reference these
 * columns as if they were defined in the schema. Calling the [.extend]
 * method creates a temporarily extended table schema.
 *
 *
 * If the table implements extended interfaces such as
 * [org.apache.calcite.schema.ScannableTable],
 * [org.apache.calcite.schema.FilterableTable] or
 * [org.apache.calcite.schema.ProjectableFilterableTable], you may wish
 * to make the table returned from [.extend] implement these interfaces
 * as well.
 */
interface ExtensibleTable : Table {
    /** Returns a table that has the row type of this table plus the given
     * fields.  */
    fun extend(fields: List<RelDataTypeField?>?): Table?

    /** Returns the starting offset of the first extended column, which may differ
     * from the field count when the table stores metadata columns that are not
     * counted in the row-type field count.  */
    val extendedColumnOffset: Int
}
