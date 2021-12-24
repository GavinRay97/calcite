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
package org.apache.calcite.schema

import org.apache.calcite.materialize.Lattice

/**
 * Extension to the [Schema] interface.
 *
 *
 * Given a user-defined schema that implements the [Schema] interface,
 * Calcite creates a wrapper that implements the `SchemaPlus` interface.
 * This provides extra functionality, such as access to tables that have been
 * added explicitly.
 *
 *
 * A user-defined schema does not need to implement this interface, but by
 * the time a schema is passed to a method in a user-defined schema or
 * user-defined table, it will have been wrapped in this interface.
 *
 *
 * SchemaPlus is intended to be used by users but not instantiated by them.
 * Users should only use the SchemaPlus they are given by the system.
 * The purpose of SchemaPlus is to expose to user code, in a read only manner,
 * some of the extra information about schemas that Calcite builds up when a
 * schema is registered. It appears in several SPI calls as context; for example
 * [SchemaFactory.create] contains a
 * parent schema that might be a wrapped instance of a user-defined
 * [Schema], or indeed might not.
 */
interface SchemaPlus : Schema {
    /**
     * Returns the parent schema, or null if this schema has no parent.
     */
    @get:Nullable
    val parentSchema: SchemaPlus

    /**
     * Returns the name of this schema.
     *
     *
     * The name must not be null, and must be unique within its parent.
     * The root schema is typically named "".
     */
    val name: String?

    // override with stricter return
    @Override
    @Nullable
    override fun getSubSchema(name: String?): SchemaPlus?

    /** Adds a schema as a sub-schema of this schema, and returns the wrapped
     * object.  */
    fun add(name: String?, schema: Schema?): SchemaPlus?

    /** Adds a table to this schema.  */
    fun add(name: String?, table: Table?)

    /** Adds a function to this schema.  */
    fun add(name: String?, function: Function?)

    /** Adds a type to this schema.   */
    fun add(name: String?, type: RelProtoDataType?)

    /** Adds a lattice to this schema.  */
    fun add(name: String?, lattice: Lattice?)

    @get:Override
    override val isMutable: Boolean

    /** Returns an underlying object.  */
    fun <T : Object?> unwrap(clazz: Class<T>?): @Nullable T?
    fun setPath(path: ImmutableList<ImmutableList<String?>?>?)
    var isCacheEnabled: Boolean
}
