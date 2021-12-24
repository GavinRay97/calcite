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

import org.apache.calcite.util.Util

/**
 * Algorithm that suggests a set of initial tiles (materialized aggregate views)
 * for a given lattice.
 */
class TileSuggester(lattice: Lattice) {
    private val lattice: Lattice

    init {
        this.lattice = lattice
    }

    fun tiles(): Iterable<Lattice.Tile?> {
        val algorithm: Algorithm = MonteCarloAlgorithm()
        val pw: PrintWriter = Util.printWriter(System.out)
        val progress: Progress = TextProgress(pw)
        val statisticsProvider: StatisticsProvider = StatisticsProviderImpl(lattice)
        val f: Double = statisticsProvider.getFactRowCount()
        val map: ImmutableMap.Builder<Parameter, Object> = ImmutableMap.builder()
        if (lattice.algorithmMaxMillis >= 0) {
            map.put(
                Algorithm.ParameterEnum.timeLimitSeconds,
                Math.max(1, (lattice.algorithmMaxMillis / 1000L) as Int)
            )
        }
        map.put(Algorithm.ParameterEnum.aggregateLimit, 3)
        map.put(Algorithm.ParameterEnum.costLimit, f * 5.0)
        val schema = SchemaImpl(lattice, statisticsProvider)
        val result: Result = algorithm.run(schema, map.build(), progress)
        val tiles: ImmutableList.Builder<Lattice.Tile> = ImmutableList.builder()
        for (aggregate in result.getAggregates()) {
            tiles.add(toTile(aggregate))
        }
        return tiles.build()
    }

    private fun toTile(aggregate: Aggregate): Lattice.Tile {
        val tileBuilder: Lattice.TileBuilder = TileBuilder()
        for (measure in lattice.defaultMeasures) {
            tileBuilder.addMeasure(measure)
        }
        for (attribute in aggregate.getAttributes()) {
            tileBuilder.addDimension((attribute as AttributeImpl).column)
        }
        return tileBuilder.build()
    }

    /** Implementation of [Schema] based on a [Lattice].  */
    private class SchemaImpl internal constructor(lattice: Lattice, statisticsProvider: StatisticsProvider) : Schema {
        private val statisticsProvider: StatisticsProvider
        private val table: TableImpl
        private val attributes: ImmutableList<AttributeImpl?>

        init {
            this.statisticsProvider = statisticsProvider
            table = TableImpl()
            val attributeBuilder: ImmutableList.Builder<AttributeImpl> = ImmutableList.builder()
            for (column in lattice.columns) {
                attributeBuilder.add(AttributeImpl(column, table))
            }
            attributes = attributeBuilder.build()
        }

        @get:Override
        val tables: List<Any?>
            get() = ImmutableList.of(table)

        @get:Override
        val measures: List<Any>
            get() {
                throw UnsupportedOperationException()
            }

        @get:Override
        val dimensions: List<Any?>
            get() {
                throw UnsupportedOperationException()
            }

        @Override
        fun getAttributes(): List<Attribute?> {
            return attributes
        }

        @Override
        fun getStatisticsProvider(): StatisticsProvider {
            return statisticsProvider
        }

        @get:Override
        val dialect: Dialect
            get() {
                throw UnsupportedOperationException()
            }

        @Override
        fun generateAggregateSql(
            aggregate: Aggregate?,
            columnNameList: List<String?>?
        ): String {
            throw UnsupportedOperationException()
        }
    }

    /** Implementation of [Table] based on a [Lattice].
     * There is only one table (in this sense of table) in a lattice.
     * The algorithm does not really care about tables.  */
    private class TableImpl : Table {
        @get:Override
        val label: String
            get() = "TABLE"

        @get:Nullable
        @get:Override
        val parent: Table?
            get() = null
    }

    /** Implementation of [Attribute] based on a [Lattice.Column].  */
    private class AttributeImpl(column: Lattice.Column, table: TableImpl) : Attribute {
        val column: Lattice.Column
        private val table: TableImpl

        init {
            this.column = column
            this.table = table
        }

        @Override
        override fun toString(): String {
            return getLabel()
        }

        @get:Override
        val label: String
            get() = column.alias

        @Override
        fun getTable(): Table {
            return table
        }

        @Override
        fun estimateSpace(): Double {
            return 0
        }

        @get:Nullable
        @get:Override
        val candidateColumnName: String?
            get() = null

        @Override
        @Nullable
        fun getDatatype(dialect: Dialect?): String? {
            return null
        }

        @get:Override
        val ancestorAttributes: List<Any>
            get() = ImmutableList.of()
    }

    /** Implementation of [org.pentaho.aggdes.model.StatisticsProvider]
     * that asks the lattice.  */
    private class StatisticsProviderImpl internal constructor(lattice: Lattice) : StatisticsProvider {
        private val lattice: Lattice

        init {
            this.lattice = lattice
        }

        @get:Override
        val factRowCount: Double
            get() = lattice.getFactRowCount()

        @Override
        fun getRowCount(attributes: List<Attribute?>?): Double {
            return lattice.getRowCount(
                Util.transform(attributes) { input -> (input as AttributeImpl).column })
        }

        @Override
        fun getSpace(attributes: List<Attribute?>): Double {
            return attributes.size()
        }

        @Override
        fun getLoadTime(attributes: List<Attribute?>): Double {
            return getSpace(attributes) * getRowCount(attributes)
        }
    }
}
