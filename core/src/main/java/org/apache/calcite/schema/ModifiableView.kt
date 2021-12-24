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

import org.apache.calcite.rel.type.RelDataType

/**
 * A modifiable view onto [ModifiableTable].
 *
 *
 * It describes how its columns map onto the underlying table's columns,
 * and any constraints that incoming rows must satisfy.
 *
 *
 * For example, given
 *
 * <blockquote><pre>
 * CREATE TABLE emps (empno INTEGER, gender VARCHAR(1), deptno INTEGER);
 * CREATE VIEW female_emps AS
 * SELECT empno, deptno FROM emps WHERE gender = 'F';
</pre></blockquote> *
 *
 *
 * constraint is `$1 = 'F'`
 * and column mapping is `[0, 2]`.
 *
 *
 * NOTE: The current API is inefficient and experimental. It will change
 * without notice.
 */
interface ModifiableView : Table {
    /** Returns a constraint that each candidate row must satisfy.
     *
     *
     * Never null; if there is no constraint, returns "true".
     *
     * @param rexBuilder Rex builder
     * @param tableRowType Row type of the table that this view maps onto
     */
    fun getConstraint(rexBuilder: RexBuilder?, tableRowType: RelDataType?): RexNode?

    /** Returns the column mapping onto another table.
     *
     *
     * `mapping[i]` contains the column of the underlying table that the
     * `i`th column of the view comes from, or -1 if it is based on an
     * expression.
     */
    val columnMapping: ImmutableIntList?

    /** Returns the underlying table.  */
    val table: org.apache.calcite.schema.Table?

    /** Returns the full path of the underlying table.  */
    val tablePath: org.apache.calcite.schema.Path?
}
