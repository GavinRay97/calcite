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
package org.apache.calcite.sql.ddl

import org.apache.calcite.schema.ColumnStrategy
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDrop
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.parser.SqlParserPos

/**
 * Utilities concerning [SqlNode] for DDL.
 */
object SqlDdlNodes {
    /** Creates a CREATE SCHEMA.  */
    fun createSchema(
        pos: SqlParserPos?, replace: Boolean,
        ifNotExists: Boolean, name: SqlIdentifier?
    ): SqlCreateSchema {
        return SqlCreateSchema(pos, replace, ifNotExists, name)
    }

    /** Creates a CREATE FOREIGN SCHEMA.  */
    fun createForeignSchema(
        pos: SqlParserPos?,
        replace: Boolean, ifNotExists: Boolean, name: SqlIdentifier?, type: SqlNode?,
        library: SqlNode?, optionList: SqlNodeList?
    ): SqlCreateForeignSchema {
        return SqlCreateForeignSchema(
            pos, replace, ifNotExists, name, type,
            library, optionList
        )
    }

    /** Creates a CREATE TYPE.  */
    fun createType(
        pos: SqlParserPos?, replace: Boolean,
        name: SqlIdentifier?, attributeList: SqlNodeList?,
        dataTypeSpec: SqlDataTypeSpec?
    ): SqlCreateType {
        return SqlCreateType(pos, replace, name, attributeList, dataTypeSpec)
    }

    /** Creates a CREATE TABLE.  */
    fun createTable(
        pos: SqlParserPos?, replace: Boolean,
        ifNotExists: Boolean, name: SqlIdentifier?, columnList: SqlNodeList?,
        query: SqlNode?
    ): SqlCreateTable {
        return SqlCreateTable(
            pos, replace, ifNotExists, name, columnList,
            query
        )
    }

    /** Creates a CREATE VIEW.  */
    fun createView(
        pos: SqlParserPos?, replace: Boolean,
        name: SqlIdentifier?, columnList: SqlNodeList?, query: SqlNode?
    ): SqlCreateView {
        return SqlCreateView(pos, replace, name, columnList, query)
    }

    /** Creates a CREATE MATERIALIZED VIEW.  */
    fun createMaterializedView(
        pos: SqlParserPos?, replace: Boolean, ifNotExists: Boolean,
        name: SqlIdentifier?, columnList: SqlNodeList?, query: SqlNode?
    ): SqlCreateMaterializedView {
        return SqlCreateMaterializedView(
            pos, replace, ifNotExists, name,
            columnList, query
        )
    }

    /** Creates a CREATE FUNCTION.  */
    fun createFunction(
        pos: SqlParserPos?, replace: Boolean, ifNotExists: Boolean,
        name: SqlIdentifier?, className: SqlNode, usingList: SqlNodeList
    ): SqlCreateFunction {
        return SqlCreateFunction(
            pos, replace, ifNotExists, name,
            className, usingList
        )
    }

    /** Creates a DROP [ FOREIGN ] SCHEMA.  */
    fun dropSchema(
        pos: SqlParserPos?, foreign: Boolean,
        ifExists: Boolean, name: SqlIdentifier
    ): SqlDropSchema {
        return SqlDropSchema(pos, foreign, ifExists, name)
    }

    /** Creates a DROP TYPE.  */
    fun dropType(
        pos: SqlParserPos?, ifExists: Boolean,
        name: SqlIdentifier
    ): SqlDropType {
        return SqlDropType(pos, ifExists, name)
    }

    /** Creates a DROP TABLE.  */
    fun dropTable(
        pos: SqlParserPos?, ifExists: Boolean,
        name: SqlIdentifier
    ): SqlDropTable {
        return SqlDropTable(pos, ifExists, name)
    }

    /** Creates a DROP VIEW.  */
    fun dropView(
        pos: SqlParserPos?, ifExists: Boolean,
        name: SqlIdentifier
    ): SqlDrop {
        return SqlDropView(pos, ifExists, name)
    }

    /** Creates a DROP MATERIALIZED VIEW.  */
    fun dropMaterializedView(
        pos: SqlParserPos?,
        ifExists: Boolean, name: SqlIdentifier
    ): SqlDrop {
        return SqlDropMaterializedView(pos, ifExists, name)
    }

    /** Creates a DROP FUNCTION.  */
    fun dropFunction(
        pos: SqlParserPos?,
        ifExists: Boolean, name: SqlIdentifier
    ): SqlDrop {
        return SqlDropFunction(pos, ifExists, name)
    }

    /** Creates a column declaration.  */
    fun column(
        pos: SqlParserPos?, name: SqlIdentifier,
        dataType: SqlDataTypeSpec, expression: SqlNode?, strategy: ColumnStrategy
    ): SqlNode {
        return SqlColumnDeclaration(pos, name, dataType, expression, strategy)
    }

    /** Creates an attribute definition.  */
    fun attribute(
        pos: SqlParserPos?, name: SqlIdentifier,
        dataType: SqlDataTypeSpec, expression: SqlNode?, collation: SqlCollation?
    ): SqlNode {
        return SqlAttributeDefinition(pos, name, dataType, expression, collation)
    }

    /** Creates a CHECK constraint.  */
    fun check(
        pos: SqlParserPos?, name: SqlIdentifier?,
        expression: SqlNode
    ): SqlNode {
        return SqlCheckConstraint(pos, name, expression)
    }

    /** Creates a UNIQUE constraint.  */
    fun unique(
        pos: SqlParserPos?, name: SqlIdentifier?,
        columnList: SqlNodeList
    ): SqlKeyConstraint {
        return SqlKeyConstraint(pos, name, columnList)
    }

    /** Creates a PRIMARY KEY constraint.  */
    fun primary(
        pos: SqlParserPos?, name: SqlIdentifier?,
        columnList: SqlNodeList
    ): SqlKeyConstraint {
        return object : SqlKeyConstraint(pos, name, columnList) {
            @get:Override
            override val operator: SqlOperator
                get() = PRIMARY
        }
    }

    /** File type for CREATE FUNCTION.  */
    enum class FileType {
        FILE, JAR, ARCHIVE
    }
}
