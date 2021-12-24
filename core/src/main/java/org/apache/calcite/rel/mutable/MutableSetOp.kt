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
package org.apache.calcite.rel.mutable

import org.apache.calcite.plan.RelOptCluster

/** Mutable equivalent of [org.apache.calcite.rel.core.SetOp].  */
abstract class MutableSetOp protected constructor(
    cluster: RelOptCluster?, rowType: RelDataType?,
    type: MutableRelType?, inputs: List<MutableRel?>?, val isAll: Boolean
) : MutableMultiRel(cluster, rowType, type, inputs) {

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableSetOp
                && type === (obj as org.apache.calcite.rel.mutable.MutableSetOp).type && isAll == (obj as MutableSetOp).isAll && inputs.equals(
            (obj as MutableSetOp).getInputs()
        )))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(type, inputs, isAll)
    }
}
