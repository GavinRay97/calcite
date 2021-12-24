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
package org.apache.calcite.rel.metadata.janino

import org.apache.calcite.rel.metadata.MetadataDef
import org.apache.calcite.rel.metadata.MetadataHandler
import org.immutables.value.Value
import java.lang.reflect.Method
import java.util.Arrays
import java.util.Comparator
import java.util.LinkedHashMap
import java.util.List
import java.util.Map
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Generates the [MetadataHandler] code.
 */
object RelMetadataHandlerGeneratorUtil {
    private const val LICENSE = ("/*\n"
            + " * Licensed to the Apache Software Foundation (ASF) under one or more\n"
            + " * contributor license agreements.  See the NOTICE file distributed with\n"
            + " * this work for additional information regarding copyright ownership.\n"
            + " * The ASF licenses this file to you under the Apache License, Version 2.0\n"
            + " * (the \"License\"); you may not use this file except in compliance with\n"
            + " * the License.  You may obtain a copy of the License at\n"
            + " *\n"
            + " * http://www.apache.org/licenses/LICENSE-2.0\n"
            + " *\n"
            + " * Unless required by applicable law or agreed to in writing, software\n"
            + " * distributed under the License is distributed on an \"AS IS\" BASIS,\n"
            + " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
            + " * See the License for the specific language governing permissions and\n"
            + " * limitations under the License.\n"
            + " */\n")

    fun generateHandler(
        handlerClass: Class<out MetadataHandler<*>?>,
        handlers: List<MetadataHandler<*>?>
    ): HandlerNameAndGeneratedCode {
        val classPackage: String = castNonNull(RelMetadataHandlerGeneratorUtil::class.java.getPackage())
            .getName()
        val name = "GeneratedMetadata_" + simpleNameForHandler(handlerClass)
        val declaredMethods: Array<Method> = Arrays.stream(handlerClass.getDeclaredMethods())
            .filter { m -> !m.getName().equals("getDef") }.toArray { i -> arrayOfNulls<Method>(i) }
        Arrays.sort(declaredMethods, Comparator.comparing(Method::getName))
        val handlerToName: Map<MetadataHandler<*>, String> = LinkedHashMap()
        for (provider in handlers) {
            handlerToName.put(
                provider,
                "provider" + handlerToName.size()
            )
        }
        val buff = StringBuilder()
        buff.append(LICENSE)
            .append("package ").append(classPackage).append(";\n\n")

        //Class definition
        buff.append("public final class ").append(name).append("\n")
            .append("  implements ").append(handlerClass.getCanonicalName()).append(" {\n")

        //PROPERTIES
        for (i in declaredMethods.indices) {
            CacheGeneratorUtil.cacheProperties(buff, declaredMethods[i], i)
        }
        for (handlerAndName in handlerToName.entrySet()) {
            buff.append("  public final ").append(handlerAndName.getKey().getClass().getName())
                .append(' ').append(handlerAndName.getValue()).append(";\n")
        }

        //CONSTRUCTOR
        buff.append("  public ").append(name).append("(\n")
        for (handlerAndName in handlerToName.entrySet()) {
            buff.append("      ")
                .append(handlerAndName.getKey().getClass().getName())
                .append(' ')
                .append(handlerAndName.getValue())
                .append(",\n")
        }
        if (!handlerToName.isEmpty()) {
            buff.setLength(buff.length() - 2)
        }
        buff.append(") {\n")
        for (handlerName in handlerToName.values()) {
            buff.append("    this.").append(handlerName).append(" = ").append(handlerName)
                .append(";\n")
        }
        buff.append("  }\n")

        //METHODS
        getDefMethod(
            buff,
            handlerToName.values()
                .stream()
                .findFirst()
                .orElse(null)
        )
        val dispatchGenerator = DispatchGenerator(handlerToName)
        for (i in declaredMethods.indices) {
            CacheGeneratorUtil.cachedMethod(buff, declaredMethods[i], i)
            dispatchGenerator.dispatchMethod(buff, declaredMethods[i], handlers)
        }
        //End of Class
        buff.append("\n}\n")
        return ImmutableHandlerNameAndGeneratedCode.builder()
            .withHandlerName("$classPackage.$name")
            .withGeneratedCode(buff.toString())
            .build()
    }

    private fun getDefMethod(buff: StringBuilder, @Nullable handlerName: String?) {
        buff.append("  public ")
            .append(MetadataDef::class.java.getName())
            .append(" getDef() {\n")
        if (handlerName == null) {
            buff.append("    return null;")
        } else {
            buff.append("    return ")
                .append(handlerName)
                .append(".getDef();\n")
        }
        buff.append("  }\n")
    }

    private fun simpleNameForHandler(clazz: Class<out MetadataHandler<*>?>): String {
        val simpleName: String = clazz.getSimpleName()
        //Previously the pattern was to have a nested in class named Handler
        //So we need to add the parents class to get a unique name
        return if (simpleName.equals("Handler")) {
            val parts: Array<String> = clazz.getName().split("\\.|\\$")
            parts[parts.size - 2] + parts[parts.size - 1]
        } else {
            simpleName
        }
    }

    /** Contains Name and code that been generated for [MetadataHandler]. */
    @Value.Immutable(singleton = false)
    interface HandlerNameAndGeneratedCode {
        val handlerName: String?
        val generatedCode: String?
    }
}
