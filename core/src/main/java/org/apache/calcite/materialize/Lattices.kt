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
package org.apache.calcite.materialize

import org.apache.calcite.materialize.Lattice.Vertex
import org.apache.calcite.materialize.LatticeSuggester.Query

/**
 * Utilities for [Lattice], [LatticeStatisticProvider].
 */
object Lattices {
    /** Statistics provider that uses SQL.  */
    val SQL: LatticeStatisticProvider.Factory = SqlLatticeStatisticProvider.FACTORY

    /** Statistics provider that uses SQL then stores the results in a cache.  */
    val CACHED_SQL: LatticeStatisticProvider.Factory = SqlLatticeStatisticProvider.CACHED_FACTORY

    /** Statistics provider that uses a profiler.  */
    val PROFILER: LatticeStatisticProvider.Factory = ProfilerLatticeStatisticProvider.FACTORY
}
