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
 * Implementation of [SqlConformance] that delegates all methods to
 * another object. You can create a sub-class that overrides particular
 * methods.
 */
class SqlDelegatingConformance protected constructor(delegate: SqlConformance) : SqlAbstractConformance() {
    private val delegate: SqlConformance

    /** Creates a SqlDelegatingConformance.  */
    init {
        this.delegate = delegate
    }

    @get:Override
    override val isGroupByAlias: Boolean
        get() = delegate.isGroupByAlias()

    @get:Override
    override val isGroupByOrdinal: Boolean
        get() = delegate.isGroupByOrdinal()

    @get:Override
    override val isHavingAlias: Boolean
        get() = delegate.isGroupByAlias()

    @get:Override
    override val isSortByOrdinal: Boolean
        get() = delegate.isSortByOrdinal()

    @get:Override
    override val isSortByAlias: Boolean
        get() = delegate.isSortByAlias()

    @get:Override
    override val isSortByAliasObscures: Boolean
        get() = delegate.isSortByAliasObscures()

    @get:Override
    override val isFromRequired: Boolean
        get() = delegate.isFromRequired()

    @get:Override
    override val isBangEqualAllowed: Boolean
        get() = delegate.isBangEqualAllowed()

    @get:Override
    override val isMinusAllowed: Boolean
        get() = delegate.isMinusAllowed()

    @get:Override
    override val isInsertSubsetColumnsAllowed: Boolean
        get() = delegate.isInsertSubsetColumnsAllowed()

    @Override
    override fun allowNiladicParentheses(): Boolean {
        return delegate.allowNiladicParentheses()
    }

    @Override
    override fun allowAliasUnnestItems(): Boolean {
        return delegate.allowAliasUnnestItems()
    }

    @Override
    override fun semantics(): SqlLibrary {
        return delegate.semantics()
    }
}
