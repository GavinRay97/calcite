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
package org.apache.calcite.rel.externalize

import org.apache.calcite.avatica.util.TimeUnitRange

/** Registry of [Enum] classes that can be serialized to JSON.
 *
 *
 * Suppose you want to serialize the value
 * [SqlTrimFunction.Flag.LEADING] to JSON.
 * First, make sure that [SqlTrimFunction.Flag] is registered.
 * The type will be serialized as "SYMBOL".
 * The value will be serialized as the string "LEADING".
 *
 *
 * When we deserialize, we rely on the fact that the registered
 * `enum` classes have distinct values. Therefore, knowing that
 * `(type="SYMBOL", value="LEADING")` we can convert the string "LEADING"
 * to the enum `Flag.LEADING`.  */
@SuppressWarnings(["rawtypes", "unchecked"])
object RelEnumTypes {
    private val ENUM_BY_NAME: ImmutableMap<String, Enum<*>>? = null

    init {
        // Build a mapping from enum constants (e.g. LEADING) to the enum
        // that contains them (e.g. SqlTrimFunction.Flag). If there two
        // enum constants have the same name, the builder will throw.
        val enumByName: ImmutableMap.Builder<String, Enum<*>> = ImmutableMap.builder()
        register(org.apache.calcite.rel.externalize.enumByName, JoinConditionType::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, JoinType::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, Depth::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlExplainFormat::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlExplainLevel::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlInsertKeyword::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlJsonConstructorNullClause::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlJsonQueryWrapperBehavior::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlJsonValueEmptyOrErrorBehavior::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlMatchRecognize.AfterOption::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlSelectKeyword::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, SqlTrimFunction.Flag::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, TimeUnitRange::class.java)
        register(org.apache.calcite.rel.externalize.enumByName, TableModify.Operation::class.java)
        ENUM_BY_NAME = org.apache.calcite.rel.externalize.enumByName.build()
    }

    private fun register(
        builder: ImmutableMap.Builder<String, Enum<*>>,
        aClass: Class<out Enum?>
    ) {
        for (enumConstant in castNonNull(aClass.getEnumConstants())) {
            builder.put(enumConstant.name(), enumConstant)
        }
    }

    /** Converts a literal into a value that can be serialized to JSON.
     * In particular, if is an enum, converts it to its name.  */
    @Nullable
    fun fromEnum(@Nullable value: Object?): Object {
        return if (value is Enum) fromEnum(value as Enum?) else value
    }

    /** Converts an enum into its name.
     * Throws if the enum's class is not registered.  */
    fun fromEnum(enumValue: Enum): String {
        if (ENUM_BY_NAME.get(enumValue.name()) !== enumValue) {
            throw AssertionError(
                ("cannot serialize enum value to JSON: "
                        + enumValue.getDeclaringClass().getCanonicalName()) + "."
                        + enumValue
            )
        }
        return enumValue.name()
    }

    /** Converts a string to an enum value.
     * The converse of [.fromEnum].  */
    fun <E : Enum<E>?> toEnum(name: String?): E {
        return ENUM_BY_NAME.get(name)
    }
}
