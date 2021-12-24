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

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.linq4j.tree.Primitive
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.CyclicMetadataException
import org.apache.calcite.rel.metadata.DelegatingMetadataRel
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.calcite.runtime.FlatLists
import com.google.common.collect.ImmutableList
import java.lang.reflect.Method
import java.util.stream.Collectors
import org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.argList
import org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.paramList

/**
 * Generates caching code for janino backed metadata.
 */
internal object CacheGeneratorUtil {
    fun cacheProperties(buff: StringBuilder?, method: Method, methodIndex: Int) {
        selectStrategy(method).cacheProperties(buff, method, methodIndex)
    }

    fun cachedMethod(buff: StringBuilder, method: Method, methodIndex: Int) {
        val delRelClass: String = DelegatingMetadataRel::class.java.getName()
        buff.append("  public ")
            .append(method.getReturnType().getName())
            .append(" ")
            .append(method.getName())
            .append("(\n")
            .append("      ")
            .append(RelNode::class.java.getName())
            .append(" r,\n")
            .append("      ")
            .append(RelMetadataQuery::class.java.getName())
            .append(" mq")
        paramList(buff, method)
            .append(") {\n")
            .append("    while (r instanceof ").append(delRelClass).append(") {\n")
            .append("      r = ((").append(delRelClass).append(") r).getMetadataDelegateRel();\n")
            .append("    }\n")
            .append("    final Object key;\n")
        selectStrategy(method).cacheKeyBlock(buff, method, methodIndex)
        buff.append("    final Object v = mq.map.get(r, key);\n")
            .append("    if (v != null) {\n")
            .append("      if (v == ")
            .append(NullSentinel::class.java.getName())
            .append(".ACTIVE) {\n")
            .append("        throw new ")
            .append(CyclicMetadataException::class.java.getName())
            .append("();\n")
            .append("      }\n")
            .append("      if (v == ")
            .append(NullSentinel::class.java.getName())
            .append(".INSTANCE) {\n")
            .append("        return null;\n")
            .append("      }\n")
            .append("      return (")
            .append(method.getReturnType().getName())
            .append(") v;\n")
            .append("    }\n")
            .append("    mq.map.put(r, key,")
            .append(NullSentinel::class.java.getName())
            .append(".ACTIVE);\n")
            .append("    try {\n")
            .append("      final ")
            .append(method.getReturnType().getName())
            .append(" x = ")
            .append(method.getName())
            .append("_(r, mq")
        argList(buff, method)
            .append(");\n")
            .append("      mq.map.put(r, key, ")
            .append(NullSentinel::class.java.getName())
            .append(".mask(x));\n")
            .append("      return x;\n")
            .append("    } catch (")
            .append(Exception::class.java.getName())
            .append(" e) {\n")
            .append("      mq.map.row(r).clear();\n")
            .append("      throw e;\n")
            .append("    }\n")
            .append("  }\n")
            .append("\n")
    }

    private fun appendKeyName(buff: StringBuilder, methodIndex: Int) {
        buff.append("methodKey").append(methodIndex)
    }

    private fun appendKeyName(buff: StringBuilder, methodIndex: Int, arg: String) {
        buff.append("methodKey")
            .append(methodIndex).append(arg)
    }

    private fun selectStrategy(method: Method): CacheKeyStrategy {
        return when (method.getParameterCount()) {
            2 -> CacheKeyStrategy.NO_ARG
            3 -> {
                val clazz: Class<*> = method.getParameterTypes().get(2)
                if (clazz.equals(Boolean::class.javaPrimitiveType)) {
                    CacheKeyStrategy.BOOLEAN_ARG
                } else if (Enum::class.java.isAssignableFrom(clazz)) {
                    CacheKeyStrategy.ENUM_ARG
                } else if (clazz.equals(Int::class.javaPrimitiveType)) {
                    CacheKeyStrategy.INT_ARG
                } else {
                    CacheKeyStrategy.DEFAULT
                }
            }
            else -> CacheKeyStrategy.DEFAULT
        }
    }

    private fun newDescriptiveCacheKey(
        buff: StringBuilder,
        method: Method, arg: String
    ): StringBuilder {
        return buff.append("      new ")
            .append(DescriptiveCacheKey::class.java.getName())
            .append("(\"")
            .append(method.getReturnType().getSimpleName())
            .append(" ")
            .append(method.getDeclaringClass().getSimpleName())
            .append(".")
            .append(method.getName())
            .append("(")
            .append(arg)
            .append(")")
            .append("\");\n")
    }

    /**
     * Generates a set of properties that are to be used by a fragment of code to
     * efficiently create metadata keys.
     */
    private enum class CacheKeyStrategy {
        /**
         * Generates an immutable method key, then during each call instantiates a new list to all
         * the arguments.
         *
         * Example:
         * `
         * private final Object method_key_0 =
         * new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
         *
         * ...
         *
         * public java.lang.Double getDistinctRowCount(
         * org.apache.calcite.rel.RelNode r,
         * org.apache.calcite.rel.metadata.RelMetadataQuery mq,
         * org.apache.calcite.util.ImmutableBitSet a2,
         * org.apache.calcite.rex.RexNode a3) {
         * final Object key;
         * key = org.apache.calcite.runtime.FlatLists.of(method_key_0, org.apache.calcite.rel
         * .metadata.NullSentinel.mask(a2), a3);
         * final Object v = mq.map.get(r, key);
         * if (v != null) {
         * ...
        ` *
         */
        DEFAULT {
            @Override
            override fun cacheProperties(buff: StringBuilder, method: Method, methodIndex: Int) {
                val args: String = ImmutableList.copyOf(method.getParameterTypes()).stream()
                    .map(Class::getSimpleName)
                    .collect(Collectors.joining(", "))
                buff.append("  private final Object ")
                appendKeyName(buff, methodIndex)
                buff.append(" =\n")
                newDescriptiveCacheKey(buff, method, args)
            }

            @Override
            override fun cacheKeyBlock(buff: StringBuilder, method: Method, methodIndex: Int) {
                buff.append("    key = ")
                    .append(
                        (if (method.getParameterTypes().length < 6) org.apache.calcite.runtime.FlatLists::class.java else ImmutableList::class.java).getName()
                    )
                    .append(".of(")
                appendKeyName(buff, methodIndex)
                safeArgList(buff, method)
                    .append(");\n")
            }

            /** Returns e.g. ", ignoreNulls".  */
            private fun safeArgList(buff: StringBuilder, method: Method): StringBuilder {
                //We ignore the first 2 arguments since they are included other ways.
                for (t in Ord.zip(method.getParameterTypes())
                    .subList(2, method.getParameterCount())) {
                    if (Primitive.`is`(t.e) || RexNode::class.java.isAssignableFrom(t.e)) {
                        buff.append(", a").append(t.i)
                    } else {
                        buff.append(", ").append(NullSentinel::class.java.getName())
                            .append(".mask(a").append(t.i).append(")")
                    }
                }
                return buff
            }
        },

        /**
         * Generates an immutable key that is reused across all calls.
         *
         * Example:
         * `
         * private final Object method_key_0 =
         * new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
         *
         * ...
         *
         * public java.lang.Double getPercentageOriginalRows(
         * org.apache.calcite.rel.RelNode r,
         * org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
         * final Object key;
         * key = method_key_0;
         * final Object v = mq.map.get(r, key);
        ` *
         */
        NO_ARG {
            @Override
            override fun cacheProperties(buff: StringBuilder, method: Method, methodIndex: Int) {
                buff.append("  private final Object ")
                appendKeyName(buff, methodIndex)
                buff.append(" =\n")
                newDescriptiveCacheKey(buff, method, "")
            }

            @Override
            override fun cacheKeyBlock(buff: StringBuilder, method: Method?, methodIndex: Int) {
                buff.append("    key = ")
                appendKeyName(buff, methodIndex)
                buff.append(";\n")
            }
        },

        /**
         * Generates immutable cache keys for metadata calls with single enum argument.
         *
         * Example:
         * `
         * private final Object method_key_0Null =
         * new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey(
         * "Boolean isVisibleInExplain(null)");
         * private final Object[] method_key_0 =
         * org.apache.calcite.rel.metadata.janino.CacheUtil.generateEnum(
         * "Boolean isVisibleInExplain", org.apache.calcite.sql.SqlExplainLevel.values());
         *
         * ...
         *
         * public java.lang.Boolean isVisibleInExplain(
         * org.apache.calcite.rel.RelNode r,
         * org.apache.calcite.rel.metadata.RelMetadataQuery mq,
         * org.apache.calcite.sql.SqlExplainLevel a2) {
         * final Object key;
         * if (a2 == null) {
         * key = method_key_0Null;
         * } else {
         * key = method_key_0[a2.ordinal()];
         * }
        ` *
         */
        ENUM_ARG {
            @Override
            override fun cacheKeyBlock(buff: StringBuilder, method: Method?, methodIndex: Int) {
                buff.append("    if (a2 == null) {\n")
                    .append("      key = ")
                appendKeyName(buff, methodIndex, "Null")
                buff.append(";\n")
                    .append("    } else {\n")
                    .append("      key = ")
                appendKeyName(buff, methodIndex)
                buff.append("[a2.ordinal()];\n")
                    .append("    }\n")
            }

            @Override
            override fun cacheProperties(buff: StringBuilder, method: Method, methodIndex: Int) {
                assert(method.getParameterCount() === 3)
                val clazz: Class<*> = method.getParameterTypes().get(2)
                assert(Enum::class.java.isAssignableFrom(clazz))
                buff.append("  private final Object ")
                appendKeyName(buff, methodIndex, "Null")
                buff.append(" =\n")
                newDescriptiveCacheKey(buff, method, "null")
                    .append("  private final Object[] ")
                appendKeyName(buff, methodIndex)
                buff.append(" =\n")
                    .append("      ")
                    .append(CacheUtil::class.java.getName())
                    .append(".generateEnum(\"")
                    .append(method.getReturnType().getSimpleName())
                    .append(" ")
                    .append(method.getName())
                    .append("\", ")
                    .append(clazz.getName())
                    .append(".values());\n")
            }
        },

        /**
         * Generates 2 immutable keys for functions that only take a single boolean arg.
         *
         * Example:
         * `
         * private final Object method_key_0True =
         * new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
         * private final Object method_key_0False =
         * new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
         *
         * ...
         *
         * public java.util.Set getUniqueKeys(
         * org.apache.calcite.rel.RelNode r,
         * org.apache.calcite.rel.metadata.RelMetadataQuery mq,
         * boolean a2) {
         * final Object key;
         * key = a2 ? method_key_0True : method_key_0False;
         * final Object v = mq.map.get(r, key);
         * ...
        ` *
         */
        BOOLEAN_ARG {
            @Override
            override fun cacheKeyBlock(buff: StringBuilder, method: Method?, methodIndex: Int) {
                buff.append("    key = a2 ? ")
                appendKeyName(buff, methodIndex, "True")
                buff.append(" : ")
                appendKeyName(buff, methodIndex, "False")
                buff.append(";\n")
            }

            @Override
            override fun cacheProperties(buff: StringBuilder, method: Method, methodIndex: Int) {
                assert(method.getParameterCount() === 3)
                assert(method.getParameterTypes().get(2).equals(Boolean::class.javaPrimitiveType))
                buff.append("  private final Object ")
                appendKeyName(buff, methodIndex, "True")
                buff.append(" =\n")
                newDescriptiveCacheKey(buff, method, "true")
                    .append("  private final Object ")
                appendKeyName(buff, methodIndex, "False")
                buff.append(" =\n")
                newDescriptiveCacheKey(buff, method, "false")
            }
        },

        /**
         * Uses a flyweight for fixed range, otherwise instantiates a new list with the arguement in it.
         *
         * Example:
         * `
         * private final Object method_key_0 =
         * new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
         * private final Object[] method_key_0FlyWeight =
         * org.apache.calcite.rel.metadata.janino.CacheUtil.generateRange(
         * "java.util.Set getColumnOrigins", -256, 256);
         *
         * ...
         *
         * public java.util.Set getColumnOrigins(
         * org.apache.calcite.rel.RelNode r,
         * org.apache.calcite.rel.metadata.RelMetadataQuery mq,
         * int a2) {
         * final Object key;
         * if (a2 >= -256 && a2 < 256) {
         * key = method_key_0FlyWeight[a2 + 256];
         * } else {
         * key = org.apache.calcite.runtime.FlatLists.of(method_key_0, a2);
         * }
        ` *
         */
        INT_ARG {
            private val min = -256
            private val max = 256
            @Override
            override fun cacheKeyBlock(buff: StringBuilder, method: Method, methodIndex: Int) {
                assert(method.getParameterCount() === 3)
                assert(method.getParameterTypes().get(2) === Int::class.javaPrimitiveType)
                buff.append("    if (a2 >= ").append(min).append(" && a2 < ").append(max)
                    .append(") {\n")
                    .append("      key = ")
                appendKeyName(buff, methodIndex, "FlyWeight")
                buff.append("[a2 + ").append(-min).append("];\n")
                    .append("    } else {\n")
                    .append("      key = ").append(FlatLists::class.java.getName()).append(".of(")
                appendKeyName(buff, methodIndex)
                buff.append(", a2);\n")
                    .append("    }\n")
            }

            @Override
            override fun cacheProperties(buff: StringBuilder, method: Method, methodIndex: Int) {
                DEFAULT.cacheProperties(buff, method, methodIndex)
                buff.append("  private final Object[] ")
                appendKeyName(buff, methodIndex, "FlyWeight")
                buff.append(" =\n")
                    .append("      ")
                    .append(CacheUtil::class.java.getName()).append(".generateRange(\"")
                    .append(method.getReturnType().getName()).append(" ").append(method.getName())
                    .append("\", ").append(min).append(", ").append(max).append(");\n")
            }
        };

        abstract fun cacheKeyBlock(buff: StringBuilder?, method: Method?, methodIndex: Int)
        abstract fun cacheProperties(buff: StringBuilder?, method: Method?, methodIndex: Int)
    }
}
