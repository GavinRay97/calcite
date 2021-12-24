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

import org.apache.calcite.plan.RelOptTable

/**
 * RelMdCollation supplies a default implementation of
 * [RelMetadataQuery.distribution]
 * for the standard logical algebra.
 */
class RelMdDistribution  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.Distribution?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.Distribution.DEF

    /** Fallback method to deduce distribution for any relational expression not
     * handled by a more specific method.
     *
     * @param rel Relational expression
     * @return Relational expression's distribution
     */
    fun distribution(rel: RelNode?, mq: RelMetadataQuery?): RelDistribution {
        return RelDistributions.SINGLETON
    }

    fun distribution(rel: SingleRel, mq: RelMetadataQuery): RelDistribution {
        return mq.distribution(rel.getInput())
    }

    fun distribution(rel: BiRel, mq: RelMetadataQuery): RelDistribution {
        return mq.distribution(rel.getLeft())
    }

    fun distribution(rel: SetOp, mq: RelMetadataQuery): RelDistribution {
        return mq.distribution(rel.getInputs().get(0))
    }

    fun distribution(rel: TableModify, mq: RelMetadataQuery): RelDistribution {
        return mq.distribution(rel.getInput())
    }

    @Nullable
    fun distribution(scan: TableScan, mq: RelMetadataQuery?): RelDistribution {
        return table(scan.getTable())
    }

    fun distribution(project: Project, mq: RelMetadataQuery): RelDistribution {
        return project(mq, project.getInput(), project.getProjects())
    }

    fun distribution(values: Values, mq: RelMetadataQuery?): RelDistribution {
        return values(values.getRowType(), values.getTuples())
    }

    fun distribution(exchange: Exchange, mq: RelMetadataQuery?): RelDistribution {
        return exchange(exchange.distribution)
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdDistribution(), BuiltInMetadata.Distribution.Handler::class.java
        )
        // Helper methods
        /** Helper method to determine a
         * [TableScan]'s distribution.  */
        @Nullable
        fun table(table: RelOptTable): RelDistribution {
            return table.getDistribution()
        }

        /** Helper method to determine a
         * [Snapshot]'s distribution.  */
        fun snapshot(mq: RelMetadataQuery, input: RelNode?): RelDistribution {
            return mq.distribution(input)
        }

        /** Helper method to determine a
         * [Sort]'s distribution.  */
        fun sort(mq: RelMetadataQuery, input: RelNode?): RelDistribution {
            return mq.distribution(input)
        }

        /** Helper method to determine a
         * [Filter]'s distribution.  */
        fun filter(mq: RelMetadataQuery, input: RelNode?): RelDistribution {
            return mq.distribution(input)
        }

        /** Helper method to determine a
         * limit's distribution.  */
        fun limit(mq: RelMetadataQuery, input: RelNode?): RelDistribution {
            return mq.distribution(input)
        }

        /** Helper method to determine a
         * [org.apache.calcite.rel.core.Calc]'s distribution.  */
        fun calc(
            mq: RelMetadataQuery, input: RelNode,
            program: RexProgram
        ): RelDistribution {
            assert(program.getCondition() != null || !program.getProjectList().isEmpty())
            val inputDistribution: RelDistribution = mq.distribution(input)
            if (!program.getProjectList().isEmpty()) {
                val mapping: Mappings.TargetMapping = program.getPartialMapping(
                    input.getRowType().getFieldCount()
                )
                return inputDistribution.apply(mapping)
            }
            return inputDistribution
        }

        /** Helper method to determine a [Project]'s distribution.  */
        fun project(
            mq: RelMetadataQuery, input: RelNode,
            projects: List<RexNode?>?
        ): RelDistribution {
            val inputDistribution: RelDistribution = mq.distribution(input)
            val mapping: Mappings.TargetMapping = Project.getPartialMapping(
                input.getRowType().getFieldCount(),
                projects
            )
            return inputDistribution.apply(mapping)
        }

        /** Helper method to determine a
         * [Values]'s distribution.  */
        fun values(
            rowType: RelDataType?,
            tuples: ImmutableList<ImmutableList<RexLiteral?>?>?
        ): RelDistribution {
            return RelDistributions.BROADCAST_DISTRIBUTED
        }

        /** Helper method to determine an
         * [Exchange]'s
         * or [org.apache.calcite.rel.core.SortExchange]'s distribution.  */
        fun exchange(distribution: RelDistribution): RelDistribution {
            return distribution
        }
    }
}
