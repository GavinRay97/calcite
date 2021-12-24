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
package org.apache.calcite.sql.util

import org.apache.calcite.sql.SqlFunction

/**
 * ReflectiveSqlOperatorTable implements the [SqlOperatorTable] interface
 * by reflecting the public fields of a subclass.
 */
abstract class ReflectiveSqlOperatorTable  //~ Constructors -----------------------------------------------------------
protected constructor() : SqlOperatorTable {
    //~ Instance fields --------------------------------------------------------
    private val caseSensitiveOperators: Multimap<CaseSensitiveKey, SqlOperator> = HashMultimap.create()
    private val caseInsensitiveOperators: Multimap<CaseInsensitiveKey, SqlOperator> = HashMultimap.create()
    //~ Methods ----------------------------------------------------------------
    /**
     * Performs post-constructor initialization of an operator table. It can't
     * be part of the constructor, because the subclass constructor needs to
     * complete first.
     */
    fun init() {
        // Use reflection to register the expressions stored in public fields.
        for (field in getClass().getFields()) {
            try {
                if (SqlFunction::class.java.isAssignableFrom(field.getType())) {
                    val op: SqlFunction = field.get(this) as SqlFunction
                    if (op != null) {
                        register(op)
                    }
                } else if (SqlOperator::class.java.isAssignableFrom(field.getType())) {
                    val op: SqlOperator = field.get(this) as SqlOperator
                    if (op != null) {
                        register(op)
                    }
                }
            } catch (e: IllegalArgumentException) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e))
            } catch (e: IllegalAccessException) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e))
            }
        }
    }

    // implement SqlOperatorTable
    @Override
    fun lookupOperatorOverloads(
        opName: SqlIdentifier,
        @Nullable category: SqlFunctionCategory?, syntax: SqlSyntax,
        operatorList: List<SqlOperator?>, nameMatcher: SqlNameMatcher
    ) {
        // NOTE jvs 3-Mar-2005:  ignore category until someone cares
        val simpleName: String
        simpleName = if (opName.names.size() > 1) {
            if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {
                // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
                Util.last(opName.names)
            } else {
                return
            }
        } else {
            opName.getSimple()
        }
        val list: Collection<SqlOperator> = lookUpOperators(simpleName, syntax, nameMatcher)
        if (list.isEmpty()) {
            return
        }
        for (op in list) {
            if (op.getSyntax() === syntax) {
                operatorList.add(op)
            } else if (syntax === SqlSyntax.FUNCTION
                && op is SqlFunction
            ) {
                // this special case is needed for operators like CAST,
                // which are treated as functions but have special syntax
                operatorList.add(op)
            }
        }
        when (syntax) {
            BINARY, PREFIX, POSTFIX -> for (extra in lookUpOperators(simpleName, syntax, nameMatcher)) {
                // REVIEW: should only search operators added during this method?
                if (extra != null && !operatorList.contains(extra)) {
                    operatorList.add(extra)
                }
            }
            else -> {}
        }
    }

    /**
     * Look up operators based on case-sensitiveness.
     */
    private fun lookUpOperators(
        name: String, syntax: SqlSyntax,
        nameMatcher: SqlNameMatcher
    ): Collection<SqlOperator> {
        // Case sensitive only works for UDFs.
        // Always look up built-in operators case-insensitively. Even in sessions
        // with unquotedCasing=UNCHANGED and caseSensitive=true.
        return if (nameMatcher.isCaseSensitive()
            && this !is SqlStdOperatorTable
        ) {
            caseSensitiveOperators.get(
                CaseSensitiveKey(
                    name,
                    syntax
                )
            )
        } else {
            caseInsensitiveOperators.get(
                CaseInsensitiveKey(
                    name,
                    syntax
                )
            )
        }
    }

    /**
     * Registers a function or operator in the table.
     */
    fun register(op: SqlOperator) {
        // Register both for case-sensitive and case-insensitive look up.
        caseSensitiveOperators.put(CaseSensitiveKey(op.getName(), op.getSyntax()), op)
        caseInsensitiveOperators.put(CaseInsensitiveKey(op.getName(), op.getSyntax()), op)
    }

    @get:Override
    val operatorList: List<Any>
        get() = ImmutableList.copyOf(caseSensitiveOperators.values())

    /** Key for looking up operators. The name is stored in upper-case because we
     * store case-insensitively, even in a case-sensitive session.  */
    private class CaseInsensitiveKey internal constructor(name: String, syntax: SqlSyntax) :
        Pair<String?, SqlSyntax?>(name.toUpperCase(Locale.ROOT), normalize(syntax))

    /** Key for looking up operators. The name kept as what it is to look up case-sensitively.  */
    private class CaseSensitiveKey internal constructor(name: String?, syntax: SqlSyntax) :
        Pair<String?, SqlSyntax?>(name, normalize(syntax))

    companion object {
        const val IS_NAME = "INFORMATION_SCHEMA"
        private fun normalize(syntax: SqlSyntax): SqlSyntax {
            return when (syntax) {
                BINARY, PREFIX, POSTFIX -> syntax
                else -> SqlSyntax.FUNCTION
            }
        }
    }
}
