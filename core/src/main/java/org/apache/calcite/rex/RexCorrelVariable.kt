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

import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlKind
import java.util.Objects

/**
 * Reference to the current row of a correlating relational expression.
 *
 *
 * Correlating variables are introduced when performing nested loop joins.
 * Each row is received from one side of the join, a correlating variable is
 * assigned a value, and the other side of the join is restarted.
 */
class RexCorrelVariable internal constructor(
    id: CorrelationId,
    type: RelDataType?
) : RexVariable(id.getName(), type) {
    val id: CorrelationId

    //~ Constructors -----------------------------------------------------------
    init {
        this.id = Objects.requireNonNull(id, "id")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitCorrelVariable(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitCorrelVariable(this, arg)
    }

    @get:Override
    val kind: SqlKind
        get() = SqlKind.CORREL_VARIABLE

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is RexCorrelVariable
                && Objects.equals(digest, (obj as RexCorrelVariable).digest)
                && type.equals((obj as RexCorrelVariable).type)
                && id.equals((obj as RexCorrelVariable).id)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(digest, type, id)
    }
}
