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
/**
 * Optimizes relational expressions.
 *
 *&nbsp;
 *
 * <h2>Overview</h2>
 *
 *
 * A <dfn>planner</dfn> (also known as an <dfn>optimizer</dfn>) finds the
 * most efficient implementation of a
 * [relational expression][org.apache.calcite.rel.RelNode].
 *
 *
 * Interface [org.apache.calcite.plan.RelOptPlanner] defines a planner,
 * and class [org.apache.calcite.plan.volcano.VolcanoPlanner] is an
 * implementation which uses a dynamic programming technique. It is based upon
 * the Volcano optimizer [[1](#graefe93)].
 *
 *
 * Interface [org.apache.calcite.plan.RelOptCost] defines a cost
 * model; class [org.apache.calcite.plan.volcano.VolcanoCost] is
 * the implementation for a `VolcanoPlanner`.
 *
 *
 * A [org.apache.calcite.plan.volcano.RelSet] is a set of equivalent
 * relational expressions.  They are equivalent because they will produce the
 * same result for any set of input data. It is an equivalence class: two
 * expressions are in the same set if and only if they are in the same
 * `RelSet`.
 *
 *
 * One of the unique features of the optimizer is that expressions can take
 * on a variety of physical traits. Each relational expression has a set of
 * traits. Each trait is described by an implementation of
 * [org.apache.calcite.plan.RelTraitDef].  Manifestations of the trait
 * implement [org.apache.calcite.plan.RelTrait]. The most common example
 * of a trait is calling convention: the protocol used to receive and transmit
 * data. [org.apache.calcite.plan.ConventionTraitDef] defines the trait
 * and [org.apache.calcite.plan.Convention] enumerates the
 * protocols. Every relational expression has a single calling convention by
 * which it returns its results. Some examples:
 *
 *
 *  * [org.apache.calcite.adapter.jdbc.JdbcConvention] is a fairly
 * conventional convention; the results are rows from a
 * [JDBC result set][java.sql.ResultSet].
 *
 *  * [org.apache.calcite.plan.Convention.NONE] means that a
 * relational
 * expression cannot be implemented; typically there are rules which can
 * transform it to equivalent, implementable expressions.
 *
 *  * [org.apache.calcite.adapter.enumerable.EnumerableConvention]
 * implements the expression by
 * generating Java code. The code places the current row in a Java
 * variable, then
 * calls the piece of code which implements the consuming relational
 * expression.
 * For example, a Java array reader of the `names` array
 * would generate the following code:
 * <blockquote>
 * <pre>String[] names;
 * for (int i = 0; i &lt; names.length; i++) {
 * String name = names[i];
 * // ... code for consuming relational expression ...
 * }</pre>
</blockquote> *
 *
 *
 *
 *
 * New traits are added to the planner in one of two ways:
 *
 *  1. If the new trait is integral to Calcite, then each and every
 * implementation of [org.apache.calcite.rel.RelNode] should include
 * its manifestation of the trait as part of the
 * [org.apache.calcite.plan.RelTraitSet] passed to
 * [org.apache.calcite.rel.AbstractRelNode]'s constructor. It may be
 * useful to provide alternate `AbstractRelNode` constructors
 * if most relational expressions use a single manifestation of the
 * trait.
 *
 *  1. If the new trait describes some aspect of a Farrago extension, then
 * the RelNodes passed to
 * [org.apache.calcite.plan.volcano.VolcanoPlanner.setRoot]
 * should have their trait sets expanded before the
 * `setRoot(RelNode)` call.
 *
 *
 *
 *
 * The second trait extension mechanism requires that implementations of
 * [org.apache.calcite.rel.AbstractRelNode.clone] must not assume the
 * type and quantity of traits in their trait set. In either case, the new
 * `RelTraitDef` implementation must be
 * [org.apache.calcite.plan.volcano.VolcanoPlanner.addRelTraitDef]
 * registered with the planner.
 *
 *
 * A [org.apache.calcite.plan.volcano.RelSubset] is a subset of a
 * `RelSet` containing expressions which are equivalent and which
 * have the same `Convention`. Like `RelSet`, it is an
 * equivalence class.
 *
 * <h2>Related packages</h2>
 *
 *  * `<a href="../rel/package-summary.html">org.apache.calcite.rel</a>`
 * defines [relational expressions][org.apache.calcite.rel.RelNode].
 *
 *
 *
 * <h2>Details</h2>
 *
 *
 * ...
 *
 *
 * Sets merge when the result of a rule already exists in another set. This
 * implies that all of the expressions are equivalent. The RelSets are
 * merged, and so are the contained RelSubsets.
 *
 *
 * Expression registration.
 *
 *  * Expression is added to a set. We may find that an equivalent
 * expression already exists. Otherwise, this is the moment when an
 * expression becomes public, and fixed. Its digest is assigned, which
 * allows us to quickly find identical expressions.
 *
 *  * We match operands, figure out which rules are applicable, and
 * generate rule calls. The rule calls are placed on a queue, and the
 * important ones are called later.
 *
 *  * RuleCalls allow us to defer the invocation of rules. When an
 * expression is registered
 *
 *
 *
 * Algorithm
 *
 *
 * To optimize a relational expression R:
 *
 *
 * 1. Register R.
 *
 *
 * 2. Create rule-calls for all applicable rules.
 *
 *
 * 3. Rank the rule calls by importance.
 *
 *
 * 4. Call the most important rule
 *
 *
 * 5. Repeat.
 *
 *
 * **Importance**. A rule-call is important if it is likely to produce
 * better implementation of a relexp on the plan's critical path. Hence (a)
 * it produces a member of an important RelSubset, (b) its children are
 * cheap.
 *
 *
 * Conversion. Conversions are difficult because we have to work backwards
 * from the goal.
 *
 *
 * **Rule triggering**
 *
 *
 * The rules are:
 *
 *  1. `PushFilterThroughProjectRule`. Operands:
 * <blockquote>
 * <pre>Filter
 * Project</pre>
</blockquote> *
 *
 *  1. `CombineProjectsRule`. Operands:
 * <blockquote>
 * <pre>Project
 * Project</pre>
</blockquote> *
 *
 *
 *
 *
 * A rule can be triggered by a change to any of its operands. Consider the
 * rule to combine two filters into one. It would have operands [Filter
 * [Filter]].  If I register a new Filter, it will trigger the rule in 2
 * places. Consider:
 *
 * <blockquote>
 * <pre>Project (deptno)                              [exp 1, subset A]
 * Filter (gender='F')                         [exp 2, subset B]
 * Project (deptno, gender, empno)           [exp 3, subset C]
 * Project (deptno, gender, empno, salary) [exp 4, subset D]
 * TableScan (emp)                       [exp 0, subset X]</pre>
</blockquote> *
 *
 * Apply `PushFilterThroughProjectRule` to [exp 2, exp 3]:
 * <blockquote>
 * <pre>Project (deptno)                              [exp 1, subset A]
 * Project (deptno, gender, empno)             [exp 5, subset B]
 * Filter (gender='F')                       [exp 6, subset E]
 * Project (deptno, gender, empno, salary) [exp 4, subset D]
 * TableScan (emp)                       [exp 0, subset X]</pre>
</blockquote> *
 *
 *
 * Two new expressions are created. Expression 5 is in subset B (because it
 * is equivalent to expression 2), and expression 6 is in a new equivalence
 * class, subset E.
 *
 *
 * The products of a applying a rule can trigger a cascade of rules. Even in
 * this simple system (2 rules and 4 initial expressions), two more rules
 * are triggered:
 *
 *
 *
 *  * Registering exp 5 triggers `CombineProjectsRule`(exp 1,
 * exp 5), which creates
 *
 * <blockquote>
 * <pre>Project (deptno)                              [exp 7, subset A]
 * Filter (gender='F')                         [exp 6, subset E]
 * Project (deptno, gender, empno, salary)   [exp 4, subset D]
 * TableScan (emp)                         [exp 0, subset X]</pre>
</blockquote> *
 *
 *
 *  * Registering exp 6 triggers
 * `PushFilterThroughProjectRule`(exp 6, exp 4), which
 * creates
 *
 * <blockquote>
 * <pre>Project (deptno)                              [exp 1, subset A]
 * Project (deptno, gender, empno)             [exp 5, subset B]
 * Project (deptno, gender, empno, salary)   [exp 8, subset E]
 * Filter (gender='F')                     [exp 9, subset F]
 * TableScan (emp)                       [exp 0, subset X]</pre>
</blockquote> *
 *
 *
 *
 *
 * Each rule application adds additional members to existing subsets. The
 * non-singleton subsets are now A {1, 7}, B {2, 5} and E {6, 8}, and new
 * combinations are possible. For example,
 * `CombineProjectsRule`(exp 7, exp 8) further reduces the depth
 * of the tree to:
 *
 * <blockquote>
 * <pre>Project (deptno)                          [exp 10, subset A]
 * Filter (gender='F')                     [exp 9, subset F]
 * TableScan (emp)                       [exp 0, subset X]</pre>
</blockquote> *
 *
 * Todo: show how rules can cause subsets to merge.
 *
 *
 * Conclusion:
 *
 *  1. A rule can be triggered by any of its operands.
 *  1. If a subset is a child of more than one parent, it can trigger rule
 * matches for any of its parents.
 *
 *
 *  1. Registering one relexp can trigger several rules (and even the same
 * rule several times).
 *
 *  1. Firing rules can cause subsets to merge.
 *
 * <h2>References</h2>
 *
 *
 * 1. [The
 * Volcano Optimizer
 * Generator: Extensibility and Efficient Search - Goetz Graefe, William J.
 * McKenna
 * (1993)](http://citeseer.nj.nec.com/graefe93volcano.html).
 */
package org.apache.calcite.plan.volcano
