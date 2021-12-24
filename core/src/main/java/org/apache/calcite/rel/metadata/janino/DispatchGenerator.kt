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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.MetadataHandler
import org.apache.calcite.rel.metadata.RelMetadataQuery
import com.google.common.collect.ImmutableSet
import java.lang.reflect.Method
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Collection
import java.util.Comparator
import java.util.HashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.function.Function
import java.util.stream.Collectors
import org.apache.calcite.linq4j.Nullness.castNonNull
import org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.argList
import org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.paramList

/**
 * Generates the metadata dispatch to handlers.
 */
internal class DispatchGenerator(metadataHandlerToName: Map<MetadataHandler<*>, String?>) {
    private val metadataHandlerToName: Map<MetadataHandler<*>, String>

    init {
        this.metadataHandlerToName = metadataHandlerToName
    }

    fun dispatchMethod(
        buff: StringBuilder, method: Method,
        metadataHandlers: Collection<MetadataHandler<*>?>
    ) {
        val handlersToClasses: Map<MetadataHandler<*>, Set<Class<out RelNode?>>> = metadataHandlers.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity()
                ) { mh -> methodAndInstanceToImplementingClass(method, mh) })
        val delegateClassSet: Set<Class<out RelNode?>> = handlersToClasses.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet())
        val delegateClassList: List<Class<out RelNode?>> = topologicalSort(delegateClassSet)
        buff
            .append("  private ")
            .append(method.getReturnType().getName())
            .append(" ")
            .append(method.getName())
            .append("_(\n")
            .append("      ")
            .append(RelNode::class.java.getName())
            .append(" r,\n")
            .append("      ")
            .append(RelMetadataQuery::class.java.getName())
            .append(" mq")
        paramList(buff, method)
            .append(") {\n")
        if (delegateClassList.isEmpty()) {
            throwUnknown(buff.append("    "), method)
                .append("  }\n")
        } else {
            buff
                .append(
                    delegateClassList.stream()
                        .map { clazz ->
                            ifInstanceThenDispatch(
                                method,
                                metadataHandlers, handlersToClasses, clazz
                            )
                        }
                        .collect(
                            Collectors.joining(
                                "    } else if ",
                                "    if ", "    } else {\n"
                            )
                        ))
            throwUnknown(buff.append("      "), method)
                .append("    }\n")
                .append("  }\n")
        }
    }

    private fun ifInstanceThenDispatch(
        method: Method,
        metadataHandlers: Collection<MetadataHandler<*>?>,
        handlersToClasses: Map<MetadataHandler<*>, Set<Class<out RelNode?>>>,
        clazz: Class<out RelNode?>
    ): StringBuilder {
        val handlerName = findProvider(metadataHandlers, handlersToClasses, clazz)
        val buff: StringBuilder = StringBuilder()
            .append("(r instanceof ").append(clazz.getName()).append(") {\n")
            .append("      return ")
        dispatchedCall(buff, handlerName, method, clazz)
        return buff
    }

    private fun findProvider(
        metadataHandlers: Collection<MetadataHandler<*>?>,
        handlerToClasses: Map<MetadataHandler<*>, Set<Class<out RelNode?>>>,
        clazz: Class<out RelNode?>
    ): String {
        for (mh in metadataHandlers) {
            if (handlerToClasses.getOrDefault(mh, ImmutableSet.of()).contains(clazz)) {
                return castNonNull(metadataHandlerToName[mh])
            }
        }
        throw RuntimeException()
    }

    companion object {
        private fun throwUnknown(buff: StringBuilder, method: Method): StringBuilder {
            return buff
                .append("      throw new ")
                .append(IllegalArgumentException::class.java.getName())
                .append("(\"No handler for method [").append(method)
                .append("] applied to argument of type [\" + r.getClass() + ")
                .append("\"]; we recommend you create a catch-all (RelNode) handler\"")
                .append(");\n")
        }

        private fun dispatchedCall(
            buff: StringBuilder, handlerName: String, method: Method,
            clazz: Class<out RelNode?>
        ) {
            buff.append(handlerName).append(".").append(method.getName())
                .append("((").append(clazz.getName()).append(") r, mq")
            argList(buff, method)
            buff.append(");\n")
        }

        private fun methodAndInstanceToImplementingClass(
            method: Method, handler: MetadataHandler<*>
        ): Set<Class<out RelNode?>> {
            val set: Set<Class<out RelNode?>> = HashSet()
            for (m in handler.getClass().getMethods()) {
                val aClass: Class<out RelNode?>? = toRelClass(method, m)
                if (aClass != null) {
                    set.add(aClass)
                }
            }
            return set
        }

        @Nullable
        private fun toRelClass(
            superMethod: Method,
            candidate: Method
        ): Class<out RelNode?>? {
            return if (!superMethod.getName().equals(candidate.getName())) {
                null
            } else if (superMethod.getParameterCount() !== candidate.getParameterCount()) {
                null
            } else {
                val cpt: Array<Class<*>> = candidate.getParameterTypes()
                val smpt: Array<Class<*>> = superMethod.getParameterTypes()
                if (!RelNode::class.java.isAssignableFrom(cpt[0])) {
                    return null
                } else if (!RelMetadataQuery::class.java.equals(cpt[1])) {
                    return null
                }
                for (i in 2 until smpt.size) {
                    if (cpt[i] !== smpt[i]) {
                        return null
                    }
                }
                cpt[0] as Class<out RelNode?>
            }
        }

        private fun topologicalSort(
            list: Collection<Class<out RelNode?>>
        ): List<Class<out RelNode?>> {
            val l: List<Class<out RelNode?>> = ArrayList()
            val s: ArrayDeque<Class<out RelNode?>> = list.stream()
                .sorted(Comparator.comparing(Class::getName))
                .collect(Collectors.toCollection { ArrayDeque() })
            while (!s.isEmpty()) {
                val n: Class<out RelNode?> = s.remove()
                var found = false
                for (other in s) {
                    if (n.isAssignableFrom(other)) {
                        found = true
                        break
                    }
                }
                if (found) {
                    s.add(n)
                } else {
                    l.add(n)
                }
            }
            return l
        }
    }
}
