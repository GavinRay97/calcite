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
package org.apache.calcite.sql.advise

import org.apache.calcite.sql.validate.SqlMoniker

/**
 * This class is used to return values for
 * [(String, int, String[])][SqlAdvisor.getCompletionHints].
 */
class SqlAdvisorHint {
    /** Fully qualified object name as string.  */
    @Nullable
    val id: String?

    /** Fully qualified object name as array of names.  */
    val names: @Nullable Array<String>?

    /** One of [org.apache.calcite.sql.validate.SqlMonikerType].  */
    val type: String

    constructor(id: String?, names: @Nullable Array<String>?, type: String) {
        this.id = id
        this.names = names
        this.type = type
    }

    constructor(id: SqlMoniker) {
        this.id = id.toString()
        val names: List<String> = id.getFullyQualifiedNames()
        this.names = if (names == null) null else names.toArray(arrayOfNulls<String>(0))
        type = id.getType().name()
    }
}
