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
package org.apache.calcite.rel

import org.apache.calcite.util.mapping.IntPair

/**
 * Interface for a referential constraint, i.e., Foreign-Key - Unique-Key relationship,
 * between two tables.
 */
interface RelReferentialConstraint {
    //~ Methods ----------------------------------------------------------------// to be removed before 2.0
    /** Returns the number of columns in the keys.
     *
     */
    val numColumns: Int
        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@code getColumnPairs().size()} ") get() = columnPairs.size()

    /**The qualified name of the referencing table, e.g. DEPT.  */
    val sourceQualifiedName: List<String?>?

    /** The qualified name of the referenced table, e.g. EMP.  */
    val targetQualifiedName: List<String?>?

    /** The (source, target) column ordinals.  */
    val columnPairs: List<Any?>
}
