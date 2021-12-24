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
package org.apache.calcite.rel

import org.apache.calcite.rel.core.TableFunctionScan

/**
 * Visits all the relations in a homogeneous way: always redirects calls to
 * `accept(RelNode)`.
 */
class RelHomogeneousShuttle : RelShuttleImpl() {
    @Override
    override fun visit(aggregate: LogicalAggregate?): RelNode {
        return visit(aggregate as RelNode?)
    }

    @Override
    override fun visit(match: LogicalMatch?): RelNode {
        return visit(match as RelNode?)
    }

    @Override
    override fun visit(scan: TableScan?): RelNode {
        return visit(scan as RelNode?)
    }

    @Override
    override fun visit(scan: TableFunctionScan?): RelNode {
        return visit(scan as RelNode?)
    }

    @Override
    override fun visit(values: LogicalValues?): RelNode {
        return visit(values as RelNode?)
    }

    @Override
    override fun visit(filter: LogicalFilter?): RelNode {
        return visit(filter as RelNode?)
    }

    @Override
    override fun visit(project: LogicalProject?): RelNode {
        return visit(project as RelNode?)
    }

    @Override
    override fun visit(join: LogicalJoin?): RelNode {
        return visit(join as RelNode?)
    }

    @Override
    override fun visit(correlate: LogicalCorrelate?): RelNode {
        return visit(correlate as RelNode?)
    }

    @Override
    override fun visit(union: LogicalUnion?): RelNode {
        return visit(union as RelNode?)
    }

    @Override
    override fun visit(intersect: LogicalIntersect?): RelNode {
        return visit(intersect as RelNode?)
    }

    @Override
    override fun visit(minus: LogicalMinus?): RelNode {
        return visit(minus as RelNode?)
    }

    @Override
    override fun visit(sort: LogicalSort?): RelNode {
        return visit(sort as RelNode?)
    }

    @Override
    override fun visit(exchange: LogicalExchange?): RelNode {
        return visit(exchange as RelNode?)
    }
}
