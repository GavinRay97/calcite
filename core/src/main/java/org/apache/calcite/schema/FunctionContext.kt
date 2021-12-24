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

import org.apache.calcite.linq4j.function.Experimental

/**
 * Information about a function call that is passed to the constructor of a
 * function instance.
 *
 *
 * This may enable a function to perform work up front, during construction,
 * rather than each time it is invoked. Here is an example of such a function:
 *
 * <blockquote><pre>
 * class RegexMatchesFunction {
 * final java.util.regex.Pattern pattern;
 *
 * public RegexMatchesFunction(FunctionContext cx) {
 * String pattern = cx.argumentValueAs(String.class);
 * this.compiledPattern = java.util.regex.Pattern.compile(pattern);
 * }
 *
 * public boolean eval(String pattern, String s) {
 * return this.compiledPattern.matches(s);
 * }
 * }
</pre></blockquote> *
 *
 *
 * Register it in the model as follows:
 *
 * <blockquote><pre>
 * functions: [
 * {
 * name: 'REGEX_MATCHES',
 * className: 'com.example.RegexMatchesFun'
 * }
 * ]
</pre></blockquote> *
 *
 *
 * and use it in a query:
 *
 * <blockquote><pre>
 * SELECT empno, ename
 * FROM Emp
 * WHERE regex_matches('J.*ES', ename);
 *
 * +-------+--------+
 * | EMPNO | ENAME  |
 * +-------+--------+
 * | 7900  | JAMES  |
 * | 7566  | JONES  |
 * +-------+--------+
</pre></blockquote> *
 *
 *
 * When executing the query, Calcite will create an instance of
 * `RegexMatchesFunction` and call the `eval` method on that
 * instance once per row.
 *
 *
 * If the `eval` method was static, or if the function's
 * constructor had zero parameters, the `eval` method would have to call
 * `java.util.regex.Pattern.compile(pattern)` to compile the pattern each
 * time.
 *
 *
 * This interface is marked [Experimental], which means that we may
 * change or remove methods, or the entire interface, without notice. But
 * probably we will add methods over time, which will just your UDFs more
 * information to work with.
 */
@Experimental
interface FunctionContext {
    /** Returns the type factory.  */
    val typeFactory: RelDataTypeFactory?

    /** Returns the number of parameters.  */
    val parameterCount: Int

    /** Returns whether the value of an argument is constant.
     *
     * @param ordinal Argument ordinal, starting from 0
     */
    fun isArgumentConstant(ordinal: Int): Boolean

    /** Returns the value of an argument to this function,
     * null if the argument is the NULL literal.
     *
     * @param ordinal Argument ordinal, starting from 0
     * @param valueClass Type of value
     *
     * @throws ClassCastException if argument cannot be converted to
     * `valueClass`
     *
     * @throws IllegalArgumentException if argument is not constant
     */
    fun <V> getArgumentValueAs(ordinal: Int, valueClass: Class<V>?): @Nullable V?
}
