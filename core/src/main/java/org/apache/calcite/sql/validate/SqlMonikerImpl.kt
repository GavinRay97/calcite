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
 * A generic implementation of [SqlMoniker].
 */
class SqlMonikerImpl(names: List<String?>?, type: SqlMonikerType?) : SqlMoniker {
    //~ Instance fields --------------------------------------------------------
    private val names: ImmutableList<String>
    private override val type: SqlMonikerType
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a moniker with an array of names.
     */
    init {
        this.names = ImmutableList.copyOf(names)
        this.type = Objects.requireNonNull(type, "type")
    }

    /**
     * Creates a moniker with a single name.
     */
    constructor(name: String?, type: SqlMonikerType?) : this(ImmutableList.of(name), type) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is SqlMonikerImpl
                && type === (obj as SqlMonikerImpl).type && names.equals((obj as SqlMonikerImpl).names)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(type, names)
    }

    @Override
    fun getType(): SqlMonikerType {
        return type
    }

    @get:Override
    override val fullyQualifiedNames: List<String?>?
        get() = names

    @Override
    override fun toIdentifier(): SqlIdentifier {
        return SqlIdentifier(names, SqlParserPos.ZERO)
    }

    @Override
    override fun toString(): String {
        return Util.sepList(names, ".")
    }

    @Override
    override fun id(): String {
        return type.toString() + "(" + this + ")"
    }
}
