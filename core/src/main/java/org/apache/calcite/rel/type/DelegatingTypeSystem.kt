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
package org.apache.calcite.rel.type

import org.apache.calcite.sql.type.SqlTypeName

/** Implementation of [org.apache.calcite.rel.type.RelDataTypeSystem]
 * that sends all methods to an underlying object.  */
class DelegatingTypeSystem protected constructor(typeSystem: RelDataTypeSystem) : RelDataTypeSystem {
    private val typeSystem: RelDataTypeSystem

    /** Creates a `DelegatingTypeSystem`.  */
    init {
        this.typeSystem = typeSystem
    }

    @Override
    override fun getMaxScale(typeName: SqlTypeName?): Int {
        return typeSystem.getMaxScale(typeName)
    }

    @Override
    override fun getDefaultPrecision(typeName: SqlTypeName?): Int {
        return typeSystem.getDefaultPrecision(typeName)
    }

    @Override
    override fun getMaxPrecision(typeName: SqlTypeName?): Int {
        return typeSystem.getMaxPrecision(typeName)
    }

    @Override
    override fun getMaxNumericScale(): Int {
        return typeSystem.getMaxNumericScale()
    }

    @Override
    override fun getMaxNumericPrecision(): Int {
        return typeSystem.getMaxNumericPrecision()
    }

    @Override
    @Nullable
    override fun getLiteral(typeName: SqlTypeName?, isPrefix: Boolean): String {
        return typeSystem.getLiteral(typeName, isPrefix)
    }

    @Override
    override fun isCaseSensitive(typeName: SqlTypeName?): Boolean {
        return typeSystem.isCaseSensitive(typeName)
    }

    @Override
    override fun isAutoincrement(typeName: SqlTypeName?): Boolean {
        return typeSystem.isAutoincrement(typeName)
    }

    @Override
    override fun getNumTypeRadix(typeName: SqlTypeName?): Int {
        return typeSystem.getNumTypeRadix(typeName)
    }

    @Override
    override fun deriveSumType(
        typeFactory: RelDataTypeFactory?,
        argumentType: RelDataType?
    ): RelDataType {
        return typeSystem.deriveSumType(typeFactory, argumentType)
    }

    @Override
    override fun deriveAvgAggType(
        typeFactory: RelDataTypeFactory?,
        argumentType: RelDataType?
    ): RelDataType {
        return typeSystem.deriveAvgAggType(typeFactory, argumentType)
    }

    @Override
    override fun deriveCovarType(
        typeFactory: RelDataTypeFactory?,
        arg0Type: RelDataType?, arg1Type: RelDataType?
    ): RelDataType {
        return typeSystem.deriveCovarType(typeFactory, arg0Type, arg1Type)
    }

    @Override
    override fun deriveFractionalRankType(typeFactory: RelDataTypeFactory?): RelDataType {
        return typeSystem.deriveFractionalRankType(typeFactory)
    }

    @Override
    override fun deriveRankType(typeFactory: RelDataTypeFactory?): RelDataType {
        return typeSystem.deriveRankType(typeFactory)
    }

    @Override
    override fun isSchemaCaseSensitive(): Boolean {
        return typeSystem.isSchemaCaseSensitive()
    }

    @Override
    override fun shouldConvertRaggedUnionTypesToVarying(): Boolean {
        return typeSystem.shouldConvertRaggedUnionTypesToVarying()
    }
}
