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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.util.ImmutableBitSet
import java.lang.reflect.Type
import java.util.List

/**
 * Information on the aggregate calculation context.
 * [AggAddContext] provides basic static information on types of arguments
 * and the return value of the aggregate being implemented.
 */
interface AggContext {
    /**
     * Returns the aggregation being implemented.
     * @return aggregation being implemented.
     */
    fun aggregation(): SqlAggFunction?

    /**
     * Returns the return type of the aggregate as
     * [org.apache.calcite.rel.type.RelDataType].
     * This can be helpful to test
     * [org.apache.calcite.rel.type.RelDataType.isNullable].
     *
     * @return return type of the aggregate
     */
    fun returnRelType(): RelDataType?

    /**
     * Returns the return type of the aggregate as [java.lang.reflect.Type].
     * @return return type of the aggregate as [java.lang.reflect.Type]
     */
    fun returnType(): Type?

    /**
     * Returns the parameter types of the aggregate as
     * [org.apache.calcite.rel.type.RelDataType].
     * This can be helpful to test
     * [org.apache.calcite.rel.type.RelDataType.isNullable].
     *
     * @return Parameter types of the aggregate
     */
    fun parameterRelTypes(): List<RelDataType?>?

    /**
     * Returns the parameter types of the aggregate as
     * [java.lang.reflect.Type].
     *
     * @return Parameter types of the aggregate
     */
    fun parameterTypes(): List<Type?>?

    /** Returns the ordinals of the input fields that make up the key.  */
    fun keyOrdinals(): List<Integer?>?

    /**
     * Returns the types of the group key as
     * [org.apache.calcite.rel.type.RelDataType].
     */
    fun keyRelTypes(): List<RelDataType?>?

    /**
     * Returns the types of the group key as
     * [java.lang.reflect.Type].
     */
    fun keyTypes(): List<Type?>?

    /** Returns the grouping sets we are aggregating on.  */
    fun groupSets(): List<ImmutableBitSet?>?
}
