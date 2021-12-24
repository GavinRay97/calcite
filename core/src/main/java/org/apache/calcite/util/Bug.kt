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
package org.apache.calcite.util

import kotlin.Throws
import org.apache.calcite.util.CaseInsensitiveComparator
import kotlin.jvm.Synchronized

/**
 * Holder for a list of constants describing which bugs which have not been
 * fixed.
 *
 *
 * You can use these constants to control the flow of your code. For example,
 * suppose that bug CALCITE-123 causes the "INSERT" statement to return an
 * incorrect row-count, and you want to disable unit tests. You might use the
 * constant in your code as follows:
 *
 * <blockquote>
 * <pre>Statement stmt = connection.createStatement();
 * int rowCount = stmt.execute(
 * "INSERT INTO FemaleEmps SELECT * FROM Emps WHERE gender = 'F'");
 * if (Bug.CALCITE_123_FIXED) {
 * assertEquals(rowCount, 5);
 * }</pre>
</blockquote> *
 *
 *
 * The usage of the constant is a convenient way to identify the impact of
 * the bug. When someone fixes the bug, they will remove the constant and all
 * usages of it. Also, the constant helps track the propagation of the fix: as
 * the fix is integrated into other branches, the constant will be removed from
 * those branches.
 *
 */
object Bug {
    //~ Static fields/initializers ---------------------------------------------
    // -----------------------------------------------------------------------
    // Developers should create new fields here, in their own section. This
    // will make merge conflicts much less likely than if everyone is
    // appending.
    const val DT239_FIXED = false
    const val DT785_FIXED = false
    // jhyde
    /**
     * Whether [issue
 * Fnl-3](http://issues.eigenbase.org/browse/FNL-3) is fixed.
     */
    const val FNL3_FIXED = false

    /**
     * Whether [issue
 * FRG-327: AssertionError while translating IN list that contains null](http://issues.eigenbase.org/browse/FRG-327)
     * is fixed.
     */
    const val FRG327_FIXED = false

    /**
     * Whether [issue
 * FRG-377: Regular character set identifiers defined in SQL:2008 spec like
 * :ALPHA:, * :UPPER:, :LOWER:, ... etc. are not yet implemented in
 * SIMILAR TO expressions.](http://issues.eigenbase.org/browse/FRG-377) is fixed.
     */
    const val FRG377_FIXED = false

    /**
     * Whether dtbug1684 "CURRENT_DATE not implemented in fennel calc" is fixed.
     */
    const val DT1684_FIXED = false

    /**
     * Whether [issue
 * FNL-25](http://issues.eigenbase.org/browse/FNL-25) is fixed. (also filed as dtbug 153)
     */
    const val FNL25_FIXED = false

    /**
     * Whether [issue FRG-73:
 * miscellaneous bugs with nested comments](http://issues.eigenbase.org/browse/FRG-73) is fixed.
     */
    const val FRG73_FIXED = false

    /**
     * Whether [issue FRG-78:
 * collation clause should be on expression instead of identifier](http://issues.eigenbase.org/browse/FRG-78) is
     * fixed.
     */
    const val FRG78_FIXED = false

    /**
     * Whether [issue
 * FRG-189: FarragoAutoVmOperatorTest.testSelect fails](http://issues.eigenbase.org/browse/FRG-189) is fixed.
     */
    const val FRG189_FIXED = false

    /**
     * Whether [issue
 * FRG-254: environment-dependent failure for
 * SqlOperatorTest.testPrefixPlusOperator](http://issues.eigenbase.org/browse/FRG-254) is fixed.
     */
    const val FRG254_FIXED = false

    /**
     * Whether [issue
 * FRG-282: Support precision in TIME and TIMESTAMP data types](http://issues.eigenbase.org/browse/FRG-282) is fixed.
     */
    const val FRG282_FIXED = false

    /**
     * Whether [issue
 * FRG-296: SUBSTRING(string FROM regexp FOR regexp)](http://issues.eigenbase.org/browse/FRG-296) is fixed.
     */
    const val FRG296_FIXED = false

    /**
     * Whether [issue
     * FRG-375: The expression VALUES ('cd' SIMILAR TO '[a-e^c]d') returns TRUE.
 * It should return FALSE.](http://issues.eigenbase.org/browse/FRG-375) is fixed.
     */
    const val FRG375_FIXED = false

    /** Whether
     * [[CALCITE-194]
 * Array items in MongoDB adapter](https://issues.apache.org/jira/browse/CALCITE-194) is fixed.  */
    const val CALCITE_194_FIXED = false

    /** Whether
     * [[CALCITE-673]
 * Timeout executing joins against MySQL](https://issues.apache.org/jira/browse/CALCITE-673) is fixed.  */
    const val CALCITE_673_FIXED = false

    /** Whether
     * [[CALCITE-1048]
 * Make metadata more robust](https://issues.apache.org/jira/browse/CALCITE-1048) is fixed.  */
    const val CALCITE_1048_FIXED = false

    /** Whether
     * [[CALCITE-1045]
 * Decorrelate sub-queries in Project and Join](https://issues.apache.org/jira/browse/CALCITE-1045) is fixed.  */
    const val CALCITE_1045_FIXED = false

    /**
     * Whether
     * [[CALCITE-2223]
 * ProjectMergeRule is infinitely matched when is applied after ProjectReduceExpressions Rule](https://issues.apache.org/jira/browse/CALCITE-2223)
     * is fixed.
     */
    const val CALCITE_2223_FIXED = false

    /** Whether
     * [[CALCITE-2400]
 * Allow standards-compliant column ordering for NATURAL JOIN and JOIN USING
 * when dynamic tables are used](https://issues.apache.org/jira/browse/CALCITE-2400) is fixed.  */
    const val CALCITE_2400_FIXED = false

    /** Whether
     * [[CALCITE-2401]
 * Improve RelMdPredicates performance](https://issues.apache.org/jira/browse/CALCITE-2401) is fixed.  */
    const val CALCITE_2401_FIXED = false

    /** Whether
     * [[CALCITE-2539]
 * Several test case not passed in CalciteSqlOperatorTest.java](https://issues.apache.org/jira/browse/CALCITE-2539) is fixed.  */
    const val CALCITE_2539_FIXED = false

    /** Whether
     * [[CALCITE-2869]
 * JSON data type support](https://issues.apache.org/jira/browse/CALCITE-2869) is fixed.  */
    const val CALCITE_2869_FIXED = false

    /** Whether
     * [[CALCITE-3243]
 * Incomplete validation of operands in JSON functions](https://issues.apache.org/jira/browse/CALCITE-3243) is fixed.  */
    const val CALCITE_3243_FIXED = false

    /** Whether
     * [[CALCITE-4204]
 * Intermittent precision in Druid results when using aggregation functions over columns of type
 * DOUBLE](https://issues.apache.org/jira/browse/CALCITE-4204) is fixed.  */
    const val CALCITE_4204_FIXED = false

    /** Whether
     * [[CALCITE-4205]
 * DruidAdapterIT#testDruidTimeFloorAndTimeParseExpressions2 fails](https://issues.apache.org/jira/browse/CALCITE-4205) is fixed.  */
    const val CALCITE_4205_FIXED = false

    /** Whether
     * [[CALCITE-4213]
 * Druid plans with small intervals should be chosen over full interval scan plus filter](https://issues.apache.org/jira/browse/CALCITE-4213) is
     * fixed.  */
    const val CALCITE_4213_FIXED = false

    /**
     * Use this to flag temporary code.
     */
    const val TODO_FIXED = false

    /**
     * Use this method to flag temporary code.
     *
     *
     * Example #1:
     * <blockquote><pre>
     * if (Bug.remark("baz fixed") == null) {
     * baz();
     * }</pre></blockquote>
     *
     *
     * Example #2:
     * <blockquote><pre>
     * /&#42;&#42; &#64;see Bug#remark Remove before checking in &#42;/
     * void uselessMethod() {}
    </pre></blockquote> *
     */
    fun <T> remark(remark: T): T {
        return remark
    }

    /**
     * Use this method to flag code that should be re-visited after upgrading
     * a component.
     *
     *
     * If the intended change is that a class or member be removed, flag
     * instead using a [Deprecated] annotation followed by a comment such as
     * "to be removed before 2.0".
     */
    fun upgrade(remark: String?): Boolean {
        Util.discard(remark)
        return false
    }
}
