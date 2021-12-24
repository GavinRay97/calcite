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
 * An implementation of [SqlMoniker] that encapsulates the normalized name
 * information of a [SqlIdentifier].
 */
class SqlIdentifierMoniker(id: SqlIdentifier?) : SqlMoniker {
    //~ Instance fields --------------------------------------------------------
    private val id: SqlIdentifier
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an SqlIdentifierMoniker.
     */
    init {
        this.id = Objects.requireNonNull(id, "id")
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val type: org.apache.calcite.sql.validate.SqlMonikerType
        get() = SqlMonikerType.COLUMN

    @get:Override
    override val fullyQualifiedNames: List<String?>?
        get() = id.names

    @Override
    override fun toIdentifier(): SqlIdentifier {
        return id
    }

    @Override
    override fun toString(): String {
        return id.toString()
    }

    @Override
    override fun id(): String {
        return id.toString()
    }
}
