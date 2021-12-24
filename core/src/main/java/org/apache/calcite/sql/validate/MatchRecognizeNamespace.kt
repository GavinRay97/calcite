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

import org.apache.calcite.rel.type.RelDataType

/**
 * Namespace for a `MATCH_RECOGNIZE` clause.
 */
class MatchRecognizeNamespace protected constructor(
    validator: SqlValidatorImpl?,
    matchRecognize: SqlMatchRecognize,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    private val matchRecognize: SqlMatchRecognize

    /** Creates a MatchRecognizeNamespace.  */
    init {
        this.matchRecognize = matchRecognize
    }

    @Override
    fun validateImpl(targetRowType: RelDataType?): RelDataType {
        validator.validateMatchRecognize(matchRecognize)
        return requireNonNull(rowType, "rowType")
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = matchRecognize
}
