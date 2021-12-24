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
package org.apache.calcite.adapter.java

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import java.lang.reflect.Type
import java.util.List

/**
 * Type factory that can register Java classes as record types.
 */
interface JavaTypeFactory : RelDataTypeFactory {
    /**
     * Creates a record type based upon the public fields of a Java class.
     *
     * @param clazz Java class
     * @return Record type that remembers its Java class
     */
    fun createStructType(clazz: Class?): RelDataType?

    /**
     * Creates a type, deducing whether a record, scalar or primitive type
     * is needed.
     *
     * @param type Java type, such as a [Class]
     * @return Record or scalar type
     */
    fun createType(type: Type?): RelDataType
    fun getJavaClass(type: RelDataType?): Type?

    /** Creates a synthetic Java class whose fields have the given Java
     * types.  */
    fun createSyntheticType(types: List<Type?>?): Type?

    /** Converts a type in Java format to a SQL-oriented type.  */
    fun toSql(type: RelDataType?): RelDataType?
}
