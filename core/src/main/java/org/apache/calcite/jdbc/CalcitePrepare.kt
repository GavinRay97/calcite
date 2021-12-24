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
package org.apache.calcite.jdbc

import org.apache.calcite.DataContext

/**
 * API for a service that prepares statements for execution.
 */
interface CalcitePrepare {
    fun parse(context: Context?, sql: String?): ParseResult?
    fun convert(context: Context?, sql: String?): ConvertResult?

    /** Executes a DDL statement.
     *
     *
     * The statement identified itself as DDL in the
     * [org.apache.calcite.jdbc.CalcitePrepare.ParseResult.kind] field.  */
    fun executeDdl(context: Context?, node: SqlNode?)

    /** Analyzes a view.
     *
     * @param context Context
     * @param sql View SQL
     * @param fail Whether to fail (and throw a descriptive error message) if the
     * view is not modifiable
     * @return Result of analyzing the view
     */
    fun analyzeView(context: Context?, sql: String?, fail: Boolean): AnalyzeViewResult?
    fun <T> prepareSql(
        context: Context?,
        query: Query<T>?,
        elementType: Type?,
        maxRowCount: Long
    ): CalciteSignature<T>

    fun <T> prepareQueryable(
        context: Context?,
        queryable: Queryable<T>?
    ): CalciteSignature<T>

    /** Context for preparing a statement.  */
    interface Context {
        val typeFactory: JavaTypeFactory?

        /** Returns the root schema for statements that need a read-consistent
         * snapshot.  */
        val rootSchema: org.apache.calcite.jdbc.CalciteSchema?

        /** Returns the root schema for statements that need to be able to modify
         * schemas and have the results available to other statements. Viz, DDL
         * statements.  */
        val mutableRootSchema: org.apache.calcite.jdbc.CalciteSchema?
        val defaultSchemaPath: List<String?>?
        fun config(): CalciteConnectionConfig?

        /** Returns the spark handler. Never null.  */
        fun spark(): SparkHandler?
        val dataContext: DataContext?

        /** Returns the path of the object being analyzed, or null.
         *
         *
         * The object is being analyzed is typically a view. If it is already
         * being analyzed further up the stack, the view definition can be deduced
         * to be cyclic.  */
        @get:Nullable
        val objectPath: List<String>?

        /** Gets a runner; it can execute a relational expression.  */
        val relRunner: RelRunner?
    }

    /** Callback to register Spark as the main engine.  */
    interface SparkHandler {
        fun flattenTypes(
            planner: RelOptPlanner?, rootRel: RelNode?,
            restructure: Boolean
        ): RelNode?

        fun registerRules(builder: RuleSetBuilder?)
        fun enabled(): Boolean
        fun compile(expr: ClassDeclaration?, s: String?): ArrayBindable?
        fun sparkContext(): Object?

        /** Allows Spark to declare the rules it needs.  */
        interface RuleSetBuilder {
            fun addRule(rule: RelOptRule?)
            fun removeRule(rule: RelOptRule?)
        }
    }

    /** Namespace that allows us to define non-abstract methods inside an
     * interface.  */
    object Dummy {
        @Nullable
        private var sparkHandler: SparkHandler? = null

        /** Returns a spark handler. Returns a trivial handler, for which
         * [SparkHandler.enabled] returns `false`, if `enable`
         * is `false` or if Spark is not on the class path. Never returns
         * null.  */
        @Synchronized
        fun getSparkHandler(enable: Boolean): SparkHandler? {
            if (sparkHandler == null) {
                sparkHandler = if (enable) createHandler() else TrivialSparkHandler()
            }
            return sparkHandler
        }

        private fun createHandler(): SparkHandler {
            return try {
                val clazz: Class<*> = Class.forName("org.apache.calcite.adapter.spark.SparkHandlerImpl")
                val method: Method = clazz.getMethod("instance")
                requireNonNull(
                    method.invoke(null)
                ) { "non-null SparkHandler expected from $method" } as SparkHandler
            } catch (e: ClassNotFoundException) {
                TrivialSparkHandler()
            } catch (e: IllegalAccessException) {
                throw RuntimeException(e)
            } catch (e: ClassCastException) {
                throw RuntimeException(e)
            } catch (e: InvocationTargetException) {
                throw RuntimeException(e)
            } catch (e: NoSuchMethodException) {
                throw RuntimeException(e)
            }
        }

        fun push(context: Context) {
            val stack: Deque<Context> = castNonNull(THREAD_CONTEXT_STACK.get())
            val path = context.objectPath
            if (path != null) {
                for (context1 in stack) {
                    val path1 = context1.objectPath
                    if (path.equals(path1)) {
                        throw CyclicDefinitionException(stack.size(), path)
                    }
                }
            }
            stack.push(context)
        }

        fun peek(): Context {
            return castNonNull(castNonNull(THREAD_CONTEXT_STACK.get()).peek())
        }

        fun pop(context: Context) {
            val x: Context = castNonNull(THREAD_CONTEXT_STACK.get()).pop()
            assert(x === context)
        }

        /** Implementation of [SparkHandler] that either does nothing or
         * throws for each method. Use this if Spark is not installed.  */
        private class TrivialSparkHandler : SparkHandler {
            @Override
            override fun flattenTypes(
                planner: RelOptPlanner?, rootRel: RelNode,
                restructure: Boolean
            ): RelNode {
                return rootRel
            }

            @Override
            override fun registerRules(builder: SparkHandler.RuleSetBuilder?) {
            }

            @Override
            override fun enabled(): Boolean {
                return false
            }

            @Override
            override fun compile(expr: ClassDeclaration?, s: String?): ArrayBindable {
                throw UnsupportedOperationException()
            }

            @Override
            override fun sparkContext(): Object {
                throw UnsupportedOperationException()
            }
        }
    }

    /** The result of parsing and validating a SQL query.  */
    class ParseResult(
        prepare: CalcitePrepareImpl, validator: SqlValidator,
        sql: String,
        sqlNode: SqlNode, rowType: RelDataType
    ) {
        val prepare: CalcitePrepareImpl
        val sql // for debug
                : String
        val sqlNode: SqlNode
        val rowType: RelDataType
        val typeFactory: RelDataTypeFactory

        init {
            this.prepare = prepare
            this.sql = sql
            this.sqlNode = sqlNode
            this.rowType = rowType
            typeFactory = validator.getTypeFactory()
        }

        /** Returns the kind of statement.
         *
         *
         * Possibilities include:
         *
         *
         *  * Queries: usually [SqlKind.SELECT], but
         * other query operators such as [SqlKind.UNION] and
         * [SqlKind.ORDER_BY] are possible
         *  * DML statements: [SqlKind.INSERT], [SqlKind.UPDATE] etc.
         *  * Session control statements: [SqlKind.COMMIT]
         *  * DDL statements: [SqlKind.CREATE_TABLE],
         * [SqlKind.DROP_INDEX]
         *
         *
         * @return Kind of statement, never null
         */
        fun kind(): SqlKind {
            return sqlNode.getKind()
        }
    }

    /** The result of parsing and validating a SQL query and converting it to
     * relational algebra.  */
    class ConvertResult(
        prepare: CalcitePrepareImpl, validator: SqlValidator,
        sql: String, sqlNode: SqlNode, rowType: RelDataType, root: RelRoot
    ) : ParseResult(prepare, validator, sql, sqlNode, rowType) {
        val root: RelRoot

        init {
            this.root = root
        }
    }

    /** The result of analyzing a view.  */
    class AnalyzeViewResult(
        prepare: CalcitePrepareImpl,
        validator: SqlValidator, sql: String, sqlNode: SqlNode,
        rowType: RelDataType, root: RelRoot, @Nullable table: Table?,
        @Nullable tablePath: ImmutableList<String?>, @Nullable constraint: RexNode,
        @Nullable columnMapping: ImmutableIntList, modifiable: Boolean
    ) : ConvertResult(prepare, validator, sql, sqlNode, rowType, root) {
        /** Not null if and only if the view is modifiable.  */
        @Nullable
        val table: Table?

        @Nullable
        val tablePath: ImmutableList<String>

        @Nullable
        val constraint: RexNode

        @Nullable
        val columnMapping: ImmutableIntList
        val modifiable: Boolean

        init {
            this.table = table
            this.tablePath = tablePath
            this.constraint = constraint
            this.columnMapping = columnMapping
            this.modifiable = modifiable
            Preconditions.checkArgument(modifiable == (table != null))
        }
    }

    /** The result of preparing a query. It gives the Avatica driver framework
     * the information it needs to create a prepared statement, or to execute a
     * statement directly, without an explicit prepare step.
     *
     * @param <T> element type
    </T> */
    class CalciteSignature<T>(
        @Nullable sql: String?,
        parameterList: List<AvaticaParameter?>?,
        internalParameters: Map<String?, Object?>?,
        @Nullable rowType: RelDataType?,
        columns: List<ColumnMetaData?>?,
        cursorFactory: Meta.CursorFactory?,
        @Nullable rootSchema: CalciteSchema?,
        collationList: List<RelCollation?>?,
        maxRowCount: Long,
        @Nullable bindable: Bindable<T>,
        statementType: Meta.StatementType?
    ) : Meta.Signature(
        columns, sql, parameterList, internalParameters, cursorFactory,
        statementType
    ) {
        @JsonIgnore
        @Nullable
        val rowType: RelDataType?

        @JsonIgnore
        @Nullable
        val rootSchema: CalciteSchema?

        @JsonIgnore
        private val collationList: List<RelCollation?>?
        private val maxRowCount: Long

        @Nullable
        private val bindable: Bindable<T>

        @Deprecated // to be removed before 2.0
        constructor(
            sql: String?, parameterList: List<AvaticaParameter?>?,
            internalParameters: Map<String?, Object?>?, rowType: RelDataType?,
            columns: List<ColumnMetaData?>?, cursorFactory: Meta.CursorFactory?,
            rootSchema: CalciteSchema?, collationList: List<RelCollation?>?,
            maxRowCount: Long, bindable: Bindable<T>
        ) : this(
            sql, parameterList, internalParameters, rowType, columns,
            cursorFactory, rootSchema, collationList, maxRowCount, bindable,
            castNonNull(null)
        ) {
        }

        init {
            this.rowType = rowType
            this.rootSchema = rootSchema
            this.collationList = collationList
            this.maxRowCount = maxRowCount
            this.bindable = bindable
        }

        fun enumerable(dataContext: DataContext?): Enumerable<T> {
            var enumerable: Enumerable<T> = castNonNull(bindable).bind(dataContext)
            if (maxRowCount >= 0) {
                // Apply limit. In JDBC 0 means "no limit". But for us, -1 means
                // "no limit", and 0 is a valid limit.
                enumerable = EnumerableDefaults.take(enumerable, maxRowCount)
            }
            return enumerable
        }

        fun getCollationList(): List<RelCollation?>? {
            return collationList
        }
    }

    /** A union type of the three possible ways of expressing a query: as a SQL
     * string, a [Queryable] or a [RelNode]. Exactly one must be
     * provided.
     *
     * @param <T> element type
    </T> */
    class Query<T> private constructor(
        @field:Nullable @param:Nullable val sql: String?,
        @Nullable queryable: Queryable<T>,
        @Nullable rel: RelNode?
    ) {
        @Nullable
        val queryable: Queryable<T>

        @Nullable
        val rel: RelNode?

        init {
            this.queryable = queryable
            this.rel = rel
            assert(
                ((if (sql == null) 0 else 1)
                        + (if (queryable == null) 0 else 1)
                        + if (rel == null) 0 else 1) == 1
            )
        }

        companion object {
            fun <T> of(sql: String?): Query<T> {
                return Query(sql, null, null)
            }

            fun <T> of(queryable: Queryable<T>): Query<T> {
                return Query(null, queryable, null)
            }

            fun <T> of(rel: RelNode?): Query<T> {
                return Query(null, null, rel)
            }
        }
    }

    companion object {
        val DEFAULT_FACTORY: Function0<CalcitePrepare> = Function0<CalcitePrepare> { CalcitePrepareImpl() }
        val THREAD_CONTEXT_STACK: ThreadLocal<Deque<Context>> = ThreadLocal.withInitial { ArrayDeque() }
    }
}
