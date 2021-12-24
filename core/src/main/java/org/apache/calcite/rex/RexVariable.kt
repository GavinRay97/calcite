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
package org.apache.calcite.rex

import org.apache.calcite.rel.type.RelDataType
import java.util.Objects

/**
 * A row-expression which references a field.
 */
abstract class RexVariable protected constructor(
    name: String?,
    type: RelDataType?
) : RexNode() {
    /**
     * Returns the name of this variable.
     */
    //~ Instance fields --------------------------------------------------------
    val name: String
    protected val type: RelDataType

    //~ Constructors -----------------------------------------------------------
    init {
        this.name = Objects.requireNonNull(name, "name")
        this.digest = Objects.requireNonNull(name, "name")
        this.type = Objects.requireNonNull(type, "type")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getType(): RelDataType {
        return type
    }
}
