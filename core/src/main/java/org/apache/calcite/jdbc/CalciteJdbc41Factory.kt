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

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Implementation of [org.apache.calcite.avatica.AvaticaFactory]
 * for Calcite and JDBC 4.1 (corresponds to JDK 1.7).
 */
@SuppressWarnings("UnusedDeclaration")
class CalciteJdbc41Factory
/** Creates a JDBC factory with given major/minor version number.  */
protected constructor(major: Int, minor: Int) : CalciteFactory(major, minor) {
    /** Creates a factory for JDBC version 4.1.  */
    constructor() : this(4, 1) {}

    @Override
    override fun newConnection(
        driver: UnregisteredDriver,
        factory: AvaticaFactory?, url: String?, info: Properties?,
        @Nullable rootSchema: CalciteSchema?, @Nullable typeFactory: JavaTypeFactory?
    ): CalciteJdbc41Connection {
        return CalciteJdbc41Connection(
            driver as Driver, factory, url, info, rootSchema, typeFactory
        )
    }

    @Override
    fun newDatabaseMetaData(
        connection: AvaticaConnection?
    ): CalciteJdbc41DatabaseMetaData {
        return CalciteJdbc41DatabaseMetaData(
            connection as CalciteConnectionImpl?
        )
    }

    @Override
    fun newStatement(
        connection: AvaticaConnection?,
        h: @Nullable Meta.StatementHandle?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CalciteJdbc41Statement {
        return CalciteJdbc41Statement(
            connection as CalciteConnectionImpl?,
            h,
            resultSetType, resultSetConcurrency,
            resultSetHoldability
        )
    }

    @Override
    @Throws(SQLException::class)
    fun newPreparedStatement(
        connection: AvaticaConnection?,
        h: @Nullable Meta.StatementHandle?,
        signature: Meta.Signature?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): AvaticaPreparedStatement {
        return CalciteJdbc41PreparedStatement(
            connection as CalciteConnectionImpl?, h,
            signature as CalcitePrepare.CalciteSignature?, resultSetType,
            resultSetConcurrency, resultSetHoldability
        )
    }

    @Override
    @Throws(SQLException::class)
    fun newResultSet(
        statement: AvaticaStatement?, state: QueryState?,
        signature: Meta.Signature?, timeZone: TimeZone?, firstFrame: Meta.Frame?
    ): CalciteResultSet {
        val metaData: ResultSetMetaData = newResultSetMetaData(statement, signature)
        return CalciteResultSet(
            statement, signature, metaData, timeZone,
            firstFrame
        )
    }

    @Override
    fun newResultSetMetaData(
        statement: AvaticaStatement?,
        signature: Meta.Signature?
    ): ResultSetMetaData {
        return AvaticaResultSetMetaData(statement, null, signature)
    }

    /** Implementation of connection for JDBC 4.1.  */
    class CalciteJdbc41Connection internal constructor(
        driver: Driver, factory: AvaticaFactory?, url: String?,
        info: Properties?, @Nullable rootSchema: CalciteSchema?,
        @Nullable typeFactory: JavaTypeFactory?
    ) : CalciteConnectionImpl(driver, factory, url, info, rootSchema, typeFactory)

    /** Implementation of statement for JDBC 4.1.  */
    class CalciteJdbc41Statement internal constructor(
        connection: CalciteConnectionImpl?,
        h: @Nullable Meta.StatementHandle?, resultSetType: Int, resultSetConcurrency: Int,
        resultSetHoldability: Int
    ) : CalciteStatement(
        connection, h, resultSetType, resultSetConcurrency,
        resultSetHoldability
    )

    /** Implementation of prepared statement for JDBC 4.1.  */
    private class CalciteJdbc41PreparedStatement internal constructor(
        connection: CalciteConnectionImpl?,
        h: @Nullable Meta.StatementHandle?, signature: CalcitePrepare.CalciteSignature?,
        resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int
    ) : CalcitePreparedStatement(
        connection, h, signature, resultSetType, resultSetConcurrency,
        resultSetHoldability
    ) {
        @Override
        @Throws(SQLException::class)
        fun setRowId(
            parameterIndex: Int,
            @Nullable x: RowId?
        ) {
            getSite(parameterIndex).setRowId(x)
        }

        @Override
        @Throws(SQLException::class)
        fun setNString(
            parameterIndex: Int, @Nullable value: String?
        ) {
            getSite(parameterIndex).setNString(value)
        }

        @Override
        @Throws(SQLException::class)
        fun setNCharacterStream(
            parameterIndex: Int,
            @Nullable value: Reader?,
            length: Long
        ) {
            getSite(parameterIndex)
                .setNCharacterStream(value, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setNClob(
            parameterIndex: Int,
            @Nullable value: NClob?
        ) {
            getSite(parameterIndex).setNClob(value)
        }

        @Override
        @Throws(SQLException::class)
        fun setClob(
            parameterIndex: Int,
            @Nullable reader: Reader?,
            length: Long
        ) {
            getSite(parameterIndex)
                .setClob(reader, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setBlob(
            parameterIndex: Int,
            @Nullable inputStream: InputStream?,
            length: Long
        ) {
            getSite(parameterIndex)
                .setBlob(inputStream, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setNClob(
            parameterIndex: Int,
            @Nullable reader: Reader?,
            length: Long
        ) {
            getSite(parameterIndex).setNClob(reader, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setSQLXML(
            parameterIndex: Int, @Nullable xmlObject: SQLXML?
        ) {
            getSite(parameterIndex).setSQLXML(xmlObject)
        }

        @Override
        @Throws(SQLException::class)
        fun setAsciiStream(
            parameterIndex: Int,
            @Nullable x: InputStream?,
            length: Long
        ) {
            getSite(parameterIndex)
                .setAsciiStream(x, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setBinaryStream(
            parameterIndex: Int,
            @Nullable x: InputStream?,
            length: Long
        ) {
            getSite(parameterIndex)
                .setBinaryStream(x, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setCharacterStream(
            parameterIndex: Int,
            @Nullable reader: Reader?,
            length: Long
        ) {
            getSite(parameterIndex)
                .setCharacterStream(reader, length)
        }

        @Override
        @Throws(SQLException::class)
        fun setAsciiStream(
            parameterIndex: Int, @Nullable x: InputStream?
        ) {
            getSite(parameterIndex).setAsciiStream(x)
        }

        @Override
        @Throws(SQLException::class)
        fun setBinaryStream(
            parameterIndex: Int, @Nullable x: InputStream?
        ) {
            getSite(parameterIndex).setBinaryStream(x)
        }

        @Override
        @Throws(SQLException::class)
        fun setCharacterStream(
            parameterIndex: Int, @Nullable reader: Reader?
        ) {
            getSite(parameterIndex)
                .setCharacterStream(reader)
        }

        @Override
        @Throws(SQLException::class)
        fun setNCharacterStream(
            parameterIndex: Int, @Nullable value: Reader?
        ) {
            getSite(parameterIndex)
                .setNCharacterStream(value)
        }

        @Override
        @Throws(SQLException::class)
        fun setClob(
            parameterIndex: Int,
            @Nullable reader: Reader?
        ) {
            getSite(parameterIndex).setClob(reader)
        }

        @Override
        @Throws(SQLException::class)
        fun setBlob(
            parameterIndex: Int, @Nullable inputStream: InputStream?
        ) {
            getSite(parameterIndex)
                .setBlob(inputStream)
        }

        @Override
        @Throws(SQLException::class)
        fun setNClob(
            parameterIndex: Int, @Nullable reader: Reader?
        ) {
            getSite(parameterIndex)
                .setNClob(reader)
        }
    }

    /** Implementation of database metadata for JDBC 4.1.  */
    class CalciteJdbc41DatabaseMetaData internal constructor(connection: CalciteConnectionImpl?) :
        AvaticaDatabaseMetaData(connection)
}
