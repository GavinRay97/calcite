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
package org.apache.calcite.rel.metadata

import org.apache.calcite.rel.RelNode

/**
 * RelMetadataProvider defines an interface for obtaining metadata about
 * relational expressions. This interface is weakly-typed and is not intended to
 * be called directly in most contexts; instead, use a strongly-typed facade
 * such as [RelMetadataQuery].
 *
 *
 * For background and motivation, see [wiki](http://wiki.eigenbase.org/RelationalExpressionMetadata).
 *
 *
 * If your provider is not a singleton, we recommend that you implement
 * [Object.equals] and [Object.hashCode] methods. This
 * makes the cache of [JaninoRelMetadataProvider] more effective.
 */
interface RelMetadataProvider {
    //~ Methods ----------------------------------------------------------------
    /**
     * Retrieves metadata of a particular type and for a particular sub-class
     * of relational expression.
     *
     *
     * The object returned is a function. It can be applied to a relational
     * expression of the given type to create a metadata object.
     *
     *
     * For example, you might call
     *
     * <blockquote><pre>
     * RelMetadataProvider provider;
     * LogicalFilter filter;
     * RexNode predicate;
     * Function&lt;RelNode, Metadata&gt; function =
     * provider.apply(LogicalFilter.class, Selectivity.class};
     * Selectivity selectivity = function.apply(filter);
     * Double d = selectivity.selectivity(predicate);
    </pre></blockquote> *
     *
     * @param relClass Type of relational expression
     * @param metadataClass Type of metadata
     * @return Function that will field a metadata instance; or null if this
     * provider cannot supply metadata of this type
     */
    @Deprecated
    @Deprecated(
        """Use {@link RelMetadataQuery}.

    """
    )
    fun  // to be removed before 2.0
            <M : Metadata?> apply(
        relClass: Class<out RelNode?>?, metadataClass: Class<out M>?
    ): @Nullable UnboundMetadata<M>?

    @Deprecated
    fun  // to be removed before 2.0
            <M : Metadata?> handlers(
        def: MetadataDef<M>?
    ): Multimap<Method?, MetadataHandler<M>?>?

    /**
     * Retrieves a list of [MetadataHandler] for implements a particular
     * [MetadataHandler].class.  The resolution order is specificity of the relNode class,
     * with preference given to handlers that occur earlier in the list.
     *
     * For instance, given a return list of {A, B, C} where A implements RelNode and Scan,
     * B implements Scan, and C implements LogicalScan and Filter.
     *
     * Scan dispatches to a.method(Scan)
     * LogicalFilter dispatches to c.method(Filter).
     * LogicalScan dispatches to c.method(LogicalScan).
     * Aggregate dispatches to a.method(RelNode).
     *
     * The behavior is undefined if the class hierarchy for dispatching is not a tree.
     */
    fun handlers(handlerClass: Class<out MetadataHandler<*>?>?): List<MetadataHandler<*>>
}
