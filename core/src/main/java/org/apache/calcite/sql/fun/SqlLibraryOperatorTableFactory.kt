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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.sql.SqlOperator

/**
 * Factory that creates operator tables that consist of functions and operators
 * for particular named libraries. For example, the following code will return
 * an operator table that contains operators for both Oracle and MySQL:
 *
 * <blockquote>
 * <pre>SqlOperatorTable opTab =
 * SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
 * EnumSet.of(SqlLibrary.ORACLE, SqlLibrary.MYSQL))</pre>
</blockquote> *
 *
 *
 * To define a new library, add a value to enum [SqlLibrary].
 *
 *
 * To add functions to a library, add the [LibraryOperator] annotation
 * to fields that of type [SqlOperator], and the library name to its
 * [LibraryOperator.libraries] field.
 */
class SqlLibraryOperatorTableFactory private constructor(vararg classes: Class) {
    /** List of classes to scan for operators.  */
    private val classes: ImmutableList<Class>
    //~ Instance fields --------------------------------------------------------
    /** A cache that returns an operator table for a given library (or set of
     * libraries).  */
    @SuppressWarnings("methodref.receiver.bound.invalid")
    private val cache: LoadingCache<ImmutableSet<SqlLibrary>, SqlOperatorTable> =
        CacheBuilder.newBuilder().build(CacheLoader.from { librarySet: ImmutableSet<SqlLibrary> -> create(librarySet) })

    init {
        this.classes = ImmutableList.copyOf(classes)
    }
    //~ Methods ----------------------------------------------------------------
    /** Creates an operator table that contains operators in the given set of
     * libraries.  */
    private fun create(librarySet: ImmutableSet<SqlLibrary>): SqlOperatorTable {
        val list: ImmutableList.Builder<SqlOperator> = ImmutableList.builder()
        var custom = false
        var standard = false
        for (library in librarySet) {
            when (library) {
                STANDARD -> standard = true
                SPATIAL -> list.addAll(SqlOperatorTables.spatialInstance().getOperatorList())
                else -> custom = true
            }
        }

        // Use reflection to register the expressions stored in public fields.
        // Skip if the only libraries asked for are "standard" or "spatial".
        if (custom) {
            for (aClass in classes) {
                for (field in aClass.getFields()) {
                    try {
                        if (SqlOperator::class.java.isAssignableFrom(field.getType())) {
                            val op: SqlOperator = requireNonNull(
                                field.get(this)
                            ) { "null value of $field for $this" } as SqlOperator
                            if (operatorIsInLibrary(op.getName(), field, librarySet)) {
                                list.add(op)
                            }
                        }
                    } catch (e: IllegalArgumentException) {
                        throw Util.throwAsRuntime(Util.causeOrSelf(e))
                    } catch (e: IllegalAccessException) {
                        throw Util.throwAsRuntime(Util.causeOrSelf(e))
                    }
                }
            }
        }
        var operatorTable: SqlOperatorTable = ListSqlOperatorTable(list.build())
        if (standard) {
            operatorTable = SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                operatorTable
            )
        }
        return operatorTable
    }

    /** Returns a SQL operator table that contains operators in the given library
     * or libraries.  */
    fun getOperatorTable(vararg libraries: SqlLibrary?): SqlOperatorTable {
        return getOperatorTable(ImmutableSet.copyOf(libraries))
    }

    /** Returns a SQL operator table that contains operators in the given set of
     * libraries.  */
    fun getOperatorTable(librarySet: Iterable<SqlLibrary?>): SqlOperatorTable {
        return try {
            cache.get(ImmutableSet.copyOf(librarySet))
        } catch (e: ExecutionException) {
            throw Util.throwAsRuntime(
                "populating SqlOperatorTable for library "
                        + librarySet, Util.causeOrSelf(e)
            )
        }
    }

    companion object {
        /** The singleton instance.  */
        val INSTANCE = SqlLibraryOperatorTableFactory(
            SqlLibraryOperators::class.java
        )

        /** Returns whether an operator is in one or more of the given libraries.  */
        private fun operatorIsInLibrary(
            operatorName: String, field: Field,
            seekLibrarySet: Set<SqlLibrary>
        ): Boolean {
            val libraryOperator: LibraryOperator = field.getAnnotation(LibraryOperator::class.java)
                ?: throw AssertionError(
                    "Operator must have annotation: "
                            + operatorName
                )
            val librarySet: Array<SqlLibrary> = libraryOperator.libraries()
            if (librarySet.size <= 0) {
                throw AssertionError(
                    "Operator must belong to at least one library: "
                            + operatorName
                )
            }
            for (library in librarySet) {
                if (seekLibrarySet.contains(library)) {
                    return true
                }
            }
            return false
        }
    }
}
