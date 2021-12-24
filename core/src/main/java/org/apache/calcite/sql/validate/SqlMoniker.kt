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
package org.apache.calcite.sql.validate

import org.apache.calcite.sql.SqlIdentifier

/**
 * An interface of an object identifier that represents a SqlIdentifier.
 */
interface SqlMoniker {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the type of object referred to by this moniker. Never null.
     */
    val type: org.apache.calcite.sql.validate.SqlMonikerType

    /**
     * Returns the array of component names.
     */
    val fullyQualifiedNames: List<String?>?

    /**
     * Creates a [SqlIdentifier] containing the fully-qualified name.
     */
    fun toIdentifier(): SqlIdentifier?
    fun id(): String?

    companion object {
        val COMPARATOR: Comparator<SqlMoniker> = object : Comparator<SqlMoniker?>() {
            val listOrdering: Ordering<Iterable<String>> = Ordering.< String > natural < String ? > ().lexicographical()

            @Override
            fun compare(o1: SqlMoniker, o2: SqlMoniker): Int {
                var c: Int = o1.type.compareTo(o2.type)
                if (c == 0) {
                    c = listOrdering.compare(
                        o1.fullyQualifiedNames,
                        o2.fullyQualifiedNames
                    )
                }
                return c
            }
        }
    }
}
