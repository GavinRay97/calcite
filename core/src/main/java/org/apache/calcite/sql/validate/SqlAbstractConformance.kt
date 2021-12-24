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

import org.apache.calcite.sql.`fun`.SqlLibrary

/**
 * Abstract base class for implementing [SqlConformance].
 *
 *
 * Every method in `SqlConformance` is implemented,
 * and behaves the same as in [SqlConformanceEnum.DEFAULT].
 */
abstract class SqlAbstractConformance : SqlConformance {
    @get:Override
    override val isLiberal: Boolean
        get() = SqlConformanceEnum.DEFAULT.isLiberal()

    @Override
    override fun allowCharLiteralAlias(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowCharLiteralAlias()
    }

    @get:Override
    override val isGroupByAlias: Boolean
        get() = SqlConformanceEnum.DEFAULT.isGroupByAlias()

    @get:Override
    override val isGroupByOrdinal: Boolean
        get() = SqlConformanceEnum.DEFAULT.isGroupByOrdinal()

    @get:Override
    override val isHavingAlias: Boolean
        get() = SqlConformanceEnum.DEFAULT.isHavingAlias()

    @get:Override
    override val isSortByOrdinal: Boolean
        get() = SqlConformanceEnum.DEFAULT.isSortByOrdinal()

    @get:Override
    override val isSortByAlias: Boolean
        get() = SqlConformanceEnum.DEFAULT.isSortByAlias()

    @get:Override
    override val isSortByAliasObscures: Boolean
        get() = SqlConformanceEnum.DEFAULT.isSortByAliasObscures()

    @get:Override
    override val isFromRequired: Boolean
        get() = SqlConformanceEnum.DEFAULT.isFromRequired()

    @Override
    override fun splitQuotedTableName(): Boolean {
        return SqlConformanceEnum.DEFAULT.splitQuotedTableName()
    }

    @Override
    override fun allowHyphenInUnquotedTableName(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowHyphenInUnquotedTableName()
    }

    @get:Override
    override val isBangEqualAllowed: Boolean
        get() = SqlConformanceEnum.DEFAULT.isBangEqualAllowed()

    @get:Override
    override val isMinusAllowed: Boolean
        get() = SqlConformanceEnum.DEFAULT.isMinusAllowed()

    @get:Override
    override val isApplyAllowed: Boolean
        get() = SqlConformanceEnum.DEFAULT.isApplyAllowed()

    @get:Override
    override val isInsertSubsetColumnsAllowed: Boolean
        get() = SqlConformanceEnum.DEFAULT.isInsertSubsetColumnsAllowed()

    @Override
    override fun allowNiladicParentheses(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowNiladicParentheses()
    }

    @Override
    override fun allowExplicitRowValueConstructor(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowExplicitRowValueConstructor()
    }

    @Override
    override fun allowExtend(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowExtend()
    }

    @get:Override
    override val isLimitStartCountAllowed: Boolean
        get() = SqlConformanceEnum.DEFAULT.isLimitStartCountAllowed()

    @get:Override
    override val isPercentRemainderAllowed: Boolean
        get() = SqlConformanceEnum.DEFAULT.isPercentRemainderAllowed()

    @Override
    override fun allowGeometry(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowGeometry()
    }

    @Override
    override fun shouldConvertRaggedUnionTypesToVarying(): Boolean {
        return SqlConformanceEnum.DEFAULT.shouldConvertRaggedUnionTypesToVarying()
    }

    @Override
    override fun allowExtendedTrim(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowExtendedTrim()
    }

    @Override
    override fun allowPluralTimeUnits(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowPluralTimeUnits()
    }

    @Override
    override fun allowQualifyingCommonColumn(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowQualifyingCommonColumn()
    }

    @Override
    override fun allowAliasUnnestItems(): Boolean {
        return SqlConformanceEnum.DEFAULT.allowAliasUnnestItems()
    }

    @Override
    override fun semantics(): SqlLibrary {
        return SqlConformanceEnum.DEFAULT.semantics()
    }
}
