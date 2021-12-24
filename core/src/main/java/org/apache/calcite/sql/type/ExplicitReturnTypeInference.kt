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
package org.apache.calcite.sql.type

import org.apache.calcite.rel.type.RelDataType

/**
 * A [SqlReturnTypeInference] which always returns the same SQL type.
 */
class ExplicitReturnTypeInference(protoType: RelProtoDataType?) : SqlReturnTypeInference {
    //~ Instance fields --------------------------------------------------------
    protected val protoType: RelProtoDataType?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an inference rule which always returns the same type object.
     *
     *
     * If the requesting type factory is different, returns a copy of the
     * type object made using [RelDataTypeFactory.copyType]
     * within the requesting type factory.
     *
     *
     * A copy of the type is required because each statement is prepared using
     * a different type factory; each type factory maintains its own cache of
     * canonical instances of each type.
     *
     * @param protoType Type object
     */
    init {
        assert(protoType != null)
        this.protoType = protoType
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType {
        return protoType.apply(opBinding.getTypeFactory())
    }
}
