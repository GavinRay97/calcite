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
package org.apache.calcite.sql.type

import org.apache.calcite.util.Util

/**
 * This class defines some utilities to build type mapping matrix
 * which would then use to construct the [SqlTypeMappingRule] rules.
 */
object SqlTypeMappingRules {
    /** Returns the [SqlTypeMappingRule] instance based on
     * the specified `coerce` to indicate whether to return as a type coercion rule.
     *
     * @param  coerce Whether to return rules with type coercion
     * @return [SqlTypeCoercionRule] instance if `coerce` is true; else
     * returns a [SqlTypeAssignmentRule] instance
     */
    fun instance(coerce: Boolean): SqlTypeMappingRule? {
        return if (coerce) {
            SqlTypeCoercionRule.instance()
        } else {
            SqlTypeAssignmentRule.instance()
        }
    }

    /** Returns a [Builder] to build the type mappings.  */
    fun builder(): Builder {
        return Builder()
    }

    /** Keeps state while building the type mappings.  */
    class Builder internal constructor() {
        val map: Map<SqlTypeName, ImmutableSet<SqlTypeName>>
        val sets: LoadingCache<Set<SqlTypeName>, ImmutableSet<SqlTypeName>>

        /** Creates an empty [Builder].  */
        init {
            map = HashMap()
            sets = CacheBuilder.newBuilder()
                .build(CacheLoader.from { set -> Sets.immutableEnumSet(set) })
        }

        /** Add a map entry to the existing [Builder] mapping.  */
        fun add(fromType: SqlTypeName?, toTypes: Set<SqlTypeName?>?) {
            try {
                map.put(fromType, sets.get(toTypes))
            } catch (e: UncheckedExecutionException) {
                throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e))
            } catch (e: ExecutionException) {
                throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e))
            }
        }

        /** Put all the type mappings to the [Builder].  */
        fun addAll(typeMapping: Map<SqlTypeName?, ImmutableSet<SqlTypeName?>?>?) {
            try {
                map.putAll(typeMapping)
            } catch (e: UncheckedExecutionException) {
                throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e))
            }
        }

        /** Copy the map values from key `typeName` and
         * returns as a [ImmutableSet.Builder].  */
        fun copyValues(typeName: SqlTypeName): ImmutableSet.Builder<SqlTypeName> {
            return ImmutableSet.< SqlTypeName > builder < SqlTypeName ? > ()
                .addAll(castNonNull(map[typeName]))
        }
    }
}
