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
package org.apache.calcite.runtime

import org.apache.calcite.sql.SqlJsonConstructorNullClause

/**
 * A collection of functions used in JSON processing.
 */
object JsonFunctions {
    private val JSON_PATH_BASE: Pattern = Pattern.compile(
        "^\\s*(?<mode>strict|lax)\\s+(?<spec>.+)$",
        Pattern.CASE_INSENSITIVE or Pattern.DOTALL or Pattern.MULTILINE
    )
    private val JSON_PATH_JSON_PROVIDER: JacksonJsonProvider = JacksonJsonProvider()
    private val JSON_PATH_MAPPING_PROVIDER: MappingProvider = JacksonMappingProvider()
    private val JSON_PRETTY_PRINTER: PrettyPrinter = DefaultPrettyPrinter().withObjectIndenter(
        DefaultIndenter.SYSTEM_LINEFEED_INSTANCE.withLinefeed("\n")
    )

    private fun isScalarObject(obj: Object): Boolean {
        if (obj is Collection) {
            return false
        }
        return if (obj is Map) {
            false
        } else true
    }

    fun jsonize(@Nullable input: Object?): String {
        return JSON_PATH_JSON_PROVIDER.toJson(input)
    }

    @Nullable
    fun dejsonize(input: String?): Object {
        return JSON_PATH_JSON_PROVIDER.parse(input)
    }

    fun jsonValueExpression(input: String?): JsonValueContext {
        return try {
            JsonValueContext.withJavaObj(dejsonize(input))
        } catch (e: Exception) {
            JsonValueContext.withException(e)
        }
    }

    fun jsonApiCommonSyntax(input: String?): JsonPathContext {
        return jsonApiCommonSyntax(jsonValueExpression(input))
    }

    fun jsonApiCommonSyntax(input: String?, pathSpec: String?): JsonPathContext {
        return jsonApiCommonSyntax(jsonValueExpression(input), pathSpec)
    }

    @JvmOverloads
    fun jsonApiCommonSyntax(input: JsonValueContext, pathSpec: String? = "strict $"): JsonPathContext {
        val mode: PathMode
        val pathStr: String?
        return try {
            val matcher: Matcher = JSON_PATH_BASE.matcher(pathSpec)
            if (!matcher.matches()) {
                mode = PathMode.STRICT
                pathStr = pathSpec
            } else {
                mode = PathMode.valueOf(castNonNull(matcher.group(1)).toUpperCase(Locale.ROOT))
                pathStr = castNonNull(matcher.group(2))
            }
            val ctx: DocumentContext
            ctx = when (mode) {
                PathMode.STRICT -> {
                    if (input.hasException()) {
                        return JsonPathContext.withStrictException(pathSpec, input.exc)
                    }
                    JsonPath.parse(
                        input.obj(),
                        Configuration
                            .builder()
                            .jsonProvider(JSON_PATH_JSON_PROVIDER)
                            .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                            .build()
                    )
                }
                PathMode.LAX -> {
                    if (input.hasException()) {
                        return JsonPathContext.withJavaObj(PathMode.LAX, null)
                    }
                    JsonPath.parse(
                        input.obj(),
                        Configuration
                            .builder()
                            .options(Option.SUPPRESS_EXCEPTIONS)
                            .jsonProvider(JSON_PATH_JSON_PROVIDER)
                            .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                            .build()
                    )
                }
                else -> throw RESOURCE.illegalJsonPathModeInPathSpec(mode.toString(), pathSpec).ex()
            }
            try {
                JsonPathContext.withJavaObj(mode, ctx.read(pathStr))
            } catch (e: Exception) {
                JsonPathContext.withStrictException(pathSpec, e)
            }
        } catch (e: Exception) {
            JsonPathContext.withUnknownException(e)
        }
    }

    @Nullable
    fun jsonExists(input: String?, pathSpec: String?): Boolean? {
        return jsonExists(jsonApiCommonSyntax(input, pathSpec))
    }

    @Nullable
    fun jsonExists(
        input: String?, pathSpec: String?,
        errorBehavior: SqlJsonExistsErrorBehavior
    ): Boolean? {
        return jsonExists(jsonApiCommonSyntax(input, pathSpec), errorBehavior)
    }

    @Nullable
    fun jsonExists(input: JsonValueContext, pathSpec: String?): Boolean? {
        return jsonExists(jsonApiCommonSyntax(input, pathSpec))
    }

    @Nullable
    fun jsonExists(
        input: JsonValueContext, pathSpec: String?,
        errorBehavior: SqlJsonExistsErrorBehavior
    ): Boolean? {
        return jsonExists(jsonApiCommonSyntax(input, pathSpec), errorBehavior)
    }

    @Nullable
    fun jsonExists(context: JsonPathContext): Boolean? {
        return jsonExists(context, SqlJsonExistsErrorBehavior.FALSE)
    }

    @Nullable
    fun jsonExists(
        context: JsonPathContext,
        errorBehavior: SqlJsonExistsErrorBehavior
    ): Boolean? {
        return if (context.hasException()) {
            when (errorBehavior) {
                TRUE -> Boolean.TRUE
                FALSE -> Boolean.FALSE
                ERROR -> throw toUnchecked(context.exc)
                UNKNOWN -> null
                else -> throw RESOURCE.illegalErrorBehaviorInJsonExistsFunc(
                    errorBehavior.toString()
                ).ex()
            }
        } else {
            context.obj != null
        }
    }

    @Nullable
    fun jsonValue(
        input: String?,
        pathSpec: String?,
        emptyBehavior: SqlJsonValueEmptyOrErrorBehavior,
        defaultValueOnEmpty: Object?,
        errorBehavior: SqlJsonValueEmptyOrErrorBehavior,
        defaultValueOnError: Object?
    ): Object? {
        return jsonValue(
            jsonApiCommonSyntax(input, pathSpec),
            emptyBehavior,
            defaultValueOnEmpty,
            errorBehavior,
            defaultValueOnError
        )
    }

    @Nullable
    fun jsonValue(
        input: JsonValueContext,
        pathSpec: String?,
        emptyBehavior: SqlJsonValueEmptyOrErrorBehavior,
        defaultValueOnEmpty: Object?,
        errorBehavior: SqlJsonValueEmptyOrErrorBehavior,
        defaultValueOnError: Object?
    ): Object? {
        return jsonValue(
            jsonApiCommonSyntax(input, pathSpec),
            emptyBehavior,
            defaultValueOnEmpty,
            errorBehavior,
            defaultValueOnError
        )
    }

    @Nullable
    fun jsonValue(
        context: JsonPathContext,
        emptyBehavior: SqlJsonValueEmptyOrErrorBehavior,
        defaultValueOnEmpty: Object?,
        errorBehavior: SqlJsonValueEmptyOrErrorBehavior,
        defaultValueOnError: Object?
    ): Object? {
        val exc: Exception
        exc = if (context.hasException()) {
            context.exc
        } else {
            val value: Object = context.obj
            if (value == null || context.mode == PathMode.LAX
                && !isScalarObject(value)
            ) {
                return when (emptyBehavior) {
                    ERROR -> throw RESOURCE.emptyResultOfJsonValueFuncNotAllowed().ex()
                    NULL -> null
                    DEFAULT -> defaultValueOnEmpty
                    else -> throw RESOURCE.illegalEmptyBehaviorInJsonValueFunc(
                        emptyBehavior.toString()
                    ).ex()
                }
            } else if (context.mode == PathMode.STRICT
                && !isScalarObject(value)
            ) {
                RESOURCE.scalarValueRequiredInStrictModeOfJsonValueFunc(
                    value.toString()
                ).ex()
            } else {
                return value
            }
        }
        return when (errorBehavior) {
            ERROR -> throw toUnchecked(exc)
            NULL -> null
            DEFAULT -> defaultValueOnError
            else -> throw RESOURCE.illegalErrorBehaviorInJsonValueFunc(
                errorBehavior.toString()
            ).ex()
        }
    }

    @Nullable
    fun jsonQuery(
        input: String?,
        pathSpec: String?,
        wrapperBehavior: SqlJsonQueryWrapperBehavior,
        emptyBehavior: SqlJsonQueryEmptyOrErrorBehavior,
        errorBehavior: SqlJsonQueryEmptyOrErrorBehavior
    ): String? {
        return jsonQuery(
            jsonApiCommonSyntax(input, pathSpec),
            wrapperBehavior, emptyBehavior, errorBehavior
        )
    }

    @Nullable
    fun jsonQuery(
        input: JsonValueContext,
        pathSpec: String?,
        wrapperBehavior: SqlJsonQueryWrapperBehavior,
        emptyBehavior: SqlJsonQueryEmptyOrErrorBehavior,
        errorBehavior: SqlJsonQueryEmptyOrErrorBehavior
    ): String? {
        return jsonQuery(
            jsonApiCommonSyntax(input, pathSpec),
            wrapperBehavior, emptyBehavior, errorBehavior
        )
    }

    @Nullable
    fun jsonQuery(
        context: JsonPathContext,
        wrapperBehavior: SqlJsonQueryWrapperBehavior,
        emptyBehavior: SqlJsonQueryEmptyOrErrorBehavior,
        errorBehavior: SqlJsonQueryEmptyOrErrorBehavior
    ): String? {
        val exc: Exception?
        if (context.hasException()) {
            exc = context.exc
        } else {
            val value: Object?
            value = if (context.obj == null) {
                null
            } else {
                when (wrapperBehavior) {
                    WITHOUT_ARRAY -> context.obj
                    WITH_UNCONDITIONAL_ARRAY -> Collections.singletonList(context.obj)
                    WITH_CONDITIONAL_ARRAY -> if (context.obj is Collection) {
                        context.obj
                    } else {
                        Collections.singletonList(context.obj)
                    }
                    else -> throw RESOURCE.illegalWrapperBehaviorInJsonQueryFunc(
                        wrapperBehavior.toString()
                    ).ex()
                }
            }
            exc = if (value == null || context.mode == PathMode.LAX
                && isScalarObject(value)
            ) {
                return when (emptyBehavior) {
                    ERROR -> throw RESOURCE.emptyResultOfJsonQueryFuncNotAllowed().ex()
                    NULL -> null
                    EMPTY_ARRAY -> "[]"
                    EMPTY_OBJECT -> "{}"
                    else -> throw RESOURCE.illegalEmptyBehaviorInJsonQueryFunc(
                        emptyBehavior.toString()
                    ).ex()
                }
            } else if (context.mode == PathMode.STRICT && isScalarObject(value)) {
                RESOURCE.arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(
                    value.toString()
                ).ex()
            } else {
                try {
                    return jsonize(value)
                } catch (e: Exception) {
                    e
                }
            }
        }
        return when (errorBehavior) {
            ERROR -> throw toUnchecked(exc)
            NULL -> null
            EMPTY_ARRAY -> "[]"
            EMPTY_OBJECT -> "{}"
            else -> throw RESOURCE.illegalErrorBehaviorInJsonQueryFunc(
                errorBehavior.toString()
            ).ex()
        }
    }

    fun jsonObject(
        nullClause: SqlJsonConstructorNullClause,
        @Nullable vararg kvs: Object?
    ): String {
        assert(kvs.size % 2 == 0)
        val map: Map<String, Object> = HashMap()
        var i = 0
        while (i < kvs.size) {
            val k = kvs[i] as String?
            val v: Object? = kvs[i + 1]
            if (k == null) {
                throw RESOURCE.nullKeyOfJsonObjectNotAllowed().ex()
            }
            if (v == null) {
                if (nullClause === SqlJsonConstructorNullClause.NULL_ON_NULL) {
                    map.put(k, null)
                }
            } else {
                map.put(k, v)
            }
            i += 2
        }
        return jsonize(map)
    }

    fun jsonObjectAggAdd(
        map: Map, k: String?, @Nullable v: Object?,
        nullClause: SqlJsonConstructorNullClause
    ) {
        if (k == null) {
            throw RESOURCE.nullKeyOfJsonObjectNotAllowed().ex()
        }
        if (v == null) {
            if (nullClause === SqlJsonConstructorNullClause.NULL_ON_NULL) {
                map.put(k, null)
            }
        } else {
            map.put(k, v)
        }
    }

    fun jsonArray(
        nullClause: SqlJsonConstructorNullClause,
        @Nullable vararg elements: Object?
    ): String {
        val list: List<Object> = ArrayList()
        for (element in elements) {
            if (element == null) {
                if (nullClause === SqlJsonConstructorNullClause.NULL_ON_NULL) {
                    list.add(null)
                }
            } else {
                list.add(element)
            }
        }
        return jsonize(list)
    }

    fun jsonArrayAggAdd(
        list: List, @Nullable element: Object?,
        nullClause: SqlJsonConstructorNullClause
    ) {
        if (element == null) {
            if (nullClause === SqlJsonConstructorNullClause.NULL_ON_NULL) {
                list.add(null)
            }
        } else {
            list.add(element)
        }
    }

    fun jsonPretty(input: String?): String {
        return jsonPretty(jsonValueExpression(input))
    }

    fun jsonPretty(input: JsonValueContext): String {
        return try {
            JSON_PATH_JSON_PROVIDER.getObjectMapper().writer(JSON_PRETTY_PRINTER)
                .writeValueAsString(input.obj)
        } catch (e: Exception) {
            throw RESOURCE.exceptionWhileSerializingToJson(Objects.toString(input.obj)).ex(e)
        }
    }

    fun jsonType(input: String?): String {
        return jsonType(jsonValueExpression(input))
    }

    fun jsonType(input: JsonValueContext): String {
        val result: String
        val `val`: Object? = input.obj
        return try {
            result = if (`val` is Integer) {
                "INTEGER"
            } else if (`val` is String) {
                "STRING"
            } else if (`val` is Float) {
                "FLOAT"
            } else if (`val` is Double) {
                "DOUBLE"
            } else if (`val` is Long) {
                "LONG"
            } else if (`val` is Boolean) {
                "BOOLEAN"
            } else if (`val` is Date) {
                "DATE"
            } else if (`val` is Map) {
                "OBJECT"
            } else if (`val` is Collection) {
                "ARRAY"
            } else if (`val` == null) {
                "NULL"
            } else {
                throw RESOURCE.invalidInputForJsonType(`val`.toString()).ex()
            }
            result
        } catch (ex: Exception) {
            throw RESOURCE.invalidInputForJsonType(`val`.toString()).ex(ex)
        }
    }

    @Nullable
    fun jsonDepth(input: String?): Integer? {
        return jsonDepth(jsonValueExpression(input))
    }

    @Nullable
    fun jsonDepth(input: JsonValueContext): Integer? {
        val result: Integer?
        val o: Object? = input.obj
        return try {
            result = if (o == null) {
                null
            } else {
                calculateDepth(o)
            }
            result
        } catch (ex: Exception) {
            throw RESOURCE.invalidInputForJsonDepth(o.toString()).ex(ex)
        }
    }

    @SuppressWarnings("JdkObsolete")
    private fun calculateDepth(o: Object): Integer {
        if (isScalarObject(o)) {
            return 1
        }
        // Note: even even though LinkedList implements Queue, it supports null values
        //
        val q: Queue<Object> = LinkedList()
        var depth = 0
        q.add(o)
        while (!q.isEmpty()) {
            val size: Int = q.size()
            for (i in 0 until size) {
                val obj: Object = q.poll()
                if (obj is Map) {
                    for (value in (obj as LinkedHashMap).values()) {
                        q.add(value)
                    }
                } else if (obj is Collection) {
                    for (value in obj) {
                        q.add(value)
                    }
                }
            }
            ++depth
        }
        return depth
    }

    @Nullable
    fun jsonLength(input: String?): Integer? {
        return jsonLength(jsonApiCommonSyntax(input))
    }

    @Nullable
    fun jsonLength(input: JsonValueContext): Integer? {
        return jsonLength(jsonApiCommonSyntax(input))
    }

    @Nullable
    fun jsonLength(input: String?, pathSpec: String?): Integer? {
        return jsonLength(jsonApiCommonSyntax(input, pathSpec))
    }

    @Nullable
    fun jsonLength(input: JsonValueContext, pathSpec: String?): Integer? {
        return jsonLength(jsonApiCommonSyntax(input, pathSpec))
    }

    @Nullable
    fun jsonLength(context: JsonPathContext): Integer? {
        val result: Integer?
        val value: Object
        try {
            if (context.hasException()) {
                throw toUnchecked(context.exc)
            }
            value = context.obj
            if (value == null) {
                result = null
            } else {
                if (value is Collection) {
                    result = (value as Collection).size()
                } else if (value is Map) {
                    result = (value as LinkedHashMap).size()
                } else if (isScalarObject(value)) {
                    result = 1
                } else {
                    result = 0
                }
            }
        } catch (ex: Exception) {
            throw RESOURCE.invalidInputForJsonLength(
                context.toString()
            ).ex(ex)
        }
        return result
    }

    fun jsonKeys(input: String?): String {
        return jsonKeys(jsonApiCommonSyntax(input))
    }

    fun jsonKeys(input: JsonValueContext): String {
        return jsonKeys(jsonApiCommonSyntax(input))
    }

    fun jsonKeys(input: String?, pathSpec: String?): String {
        return jsonKeys(jsonApiCommonSyntax(input, pathSpec))
    }

    fun jsonKeys(input: JsonValueContext, pathSpec: String?): String {
        return jsonKeys(jsonApiCommonSyntax(input, pathSpec))
    }

    fun jsonKeys(context: JsonPathContext): String {
        var list: List<String?>? = ArrayList()
        val value: Object
        try {
            if (context.hasException()) {
                throw toUnchecked(context.exc)
            }
            value = context.obj
            if (value == null || value is Collection
                || isScalarObject(value)
            ) {
                list = null
            } else if (value is Map) {
                for (key in (value as LinkedHashMap).keySet()) {
                    list.add(key.toString())
                }
            }
        } catch (ex: Exception) {
            throw RESOURCE.invalidInputForJsonKeys(
                context.toString()
            ).ex(ex)
        }
        return jsonize(list)
    }

    fun jsonRemove(input: String?, vararg pathSpecs: String?): String {
        return jsonRemove(jsonValueExpression(input), *pathSpecs)
    }

    fun jsonRemove(input: JsonValueContext, vararg pathSpecs: String?): String {
        return try {
            val ctx: DocumentContext = JsonPath.parse(
                input.obj(),
                Configuration
                    .builder()
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .jsonProvider(JSON_PATH_JSON_PROVIDER)
                    .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                    .build()
            )
            for (pathSpec in pathSpecs) {
                if (pathSpec != null && ctx.read(pathSpec) != null) {
                    ctx.delete(pathSpec)
                }
            }
            ctx.jsonString()
        } catch (ex: Exception) {
            throw RESOURCE.invalidInputForJsonRemove(
                input.toString(), Arrays.toString(pathSpecs)
            ).ex(ex)
        }
    }

    fun jsonStorageSize(input: String?): Integer {
        return jsonStorageSize(jsonValueExpression(input))
    }

    fun jsonStorageSize(input: JsonValueContext): Integer {
        return try {
            JSON_PATH_JSON_PROVIDER.getObjectMapper()
                .writeValueAsBytes(input.obj).length
        } catch (e: Exception) {
            throw RESOURCE.invalidInputForJsonStorageSize(Objects.toString(input.obj)).ex(e)
        }
    }

    fun isJsonValue(input: String?): Boolean {
        return try {
            dejsonize(input)
            true
        } catch (e: Exception) {
            false
        }
    }

    fun isJsonObject(input: String?): Boolean {
        return try {
            val o: Object = dejsonize(input)
            o is Map
        } catch (e: Exception) {
            false
        }
    }

    fun isJsonArray(input: String?): Boolean {
        return try {
            val o: Object = dejsonize(input)
            o is Collection
        } catch (e: Exception) {
            false
        }
    }

    fun isJsonScalar(input: String?): Boolean {
        return try {
            val o: Object = dejsonize(input)
            o !is Map && o !is Collection
        } catch (e: Exception) {
            false
        }
    }

    private fun toUnchecked(e: Exception?): RuntimeException {
        return Util.toUnchecked(e)
    }

    /**
     * Returned path context of JsonApiCommonSyntax, public for testing.
     */
    class JsonPathContext private constructor(mode: PathMode, @Nullable obj: Object, @Nullable exc: Exception?) {
        val mode: PathMode

        @Nullable
        val obj: Object

        @Nullable
        val exc: Exception?

        private constructor(@Nullable obj: Object, @Nullable exc: Exception) : this(PathMode.NONE, obj, exc) {}

        init {
            assert(obj == null || exc == null)
            this.mode = mode
            this.obj = obj
            this.exc = exc
        }

        @EnsuresNonNullIf(expression = "exc", result = true)
        fun hasException(): Boolean {
            return exc != null
        }

        @Override
        override fun toString(): String {
            return ("JsonPathContext{"
                    + "mode=" + mode
                    + ", obj=" + obj
                    + ", exc=" + exc
                    + '}')
        }

        companion object {
            fun withUnknownException(exc: Exception?): JsonPathContext {
                return JsonPathContext(PathMode.UNKNOWN, null, exc)
            }

            fun withStrictException(exc: Exception?): JsonPathContext {
                return JsonPathContext(PathMode.STRICT, null, exc)
            }

            fun withStrictException(pathSpec: String?, exc: Exception?): JsonPathContext {
                var exc: Exception? = exc
                if (exc.getClass() === InvalidPathException::class.java) {
                    exc = RESOURCE.illegalJsonPathSpec(pathSpec).ex()
                }
                return withStrictException(exc)
            }

            fun withJavaObj(mode: PathMode, @Nullable obj: Object?): JsonPathContext {
                if (mode == PathMode.UNKNOWN) {
                    throw RESOURCE.illegalJsonPathMode(mode.toString()).ex()
                }
                if (mode == PathMode.STRICT && obj == null) {
                    throw RESOURCE.strictPathModeRequiresNonEmptyValue().ex()
                }
                return JsonPathContext(mode, obj, null)
            }
        }
    }

    /**
     * The Java output of [org.apache.calcite.sql.fun.SqlJsonValueExpressionOperator].
     */
    class JsonValueContext private constructor(@Nullable obj: Object?, @Nullable exc: Exception?) {
        @JsonValue
        @Nullable
        val obj: Object?

        @Nullable
        val exc: Exception?

        init {
            assert(obj == null || exc == null)
            this.obj = obj
            this.exc = exc
        }

        fun obj(): Object {
            return requireNonNull(obj, "json object must not be null")
        }

        @EnsuresNonNullIf(expression = "exc", result = true)
        fun hasException(): Boolean {
            return exc != null
        }

        @Override
        override fun equals(@Nullable o: Object?): Boolean {
            if (this === o) {
                return true
            }
            if (o == null || getClass() !== o.getClass()) {
                return false
            }
            val jsonValueContext = o as JsonValueContext
            return Objects.equals(obj, jsonValueContext.obj)
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(obj)
        }

        @Override
        override fun toString(): String {
            return Objects.toString(obj)
        }

        companion object {
            fun withJavaObj(@Nullable obj: Object?): JsonValueContext {
                return JsonValueContext(obj, null)
            }

            fun withException(exc: Exception?): JsonValueContext {
                return JsonValueContext(null, exc)
            }
        }
    }

    /**
     * Path spec has two different modes: lax mode and strict mode.
     * Lax mode suppresses any thrown exception and returns null,
     * whereas strict mode throws exceptions.
     */
    enum class PathMode {
        LAX, STRICT, UNKNOWN, NONE
    }
}
