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
package org.apache.calcite.sql.type

import org.apache.calcite.sql.type.SqlOperandCountRanges.RangeImpl

/**
 * Holds constants associated with SQL types introduced after the earliest
 * version of Java supported by Farrago (this currently means anything
 * introduced in JDK 1.6 or later).
 *
 *
 * Allows us to deal sanely with type constants returned by newer JDBC
 * drivers when running a version of Farrago compiled under an old
 * version of the JDK (i.e. 1.5).
 *
 *
 * By itself, the presence of a constant here doesn't imply that farrago
 * fully supports the associated type.  This is simply a mirror of the
 * missing constant values.
 */
interface ExtraSqlTypes {
    companion object {
        // From JDK 1.6
        const val ROWID = -8
        const val NCHAR = -15
        const val NVARCHAR = -9
        const val LONGNVARCHAR = -16
        const val NCLOB = 2011
        const val SQLXML = 2009

        // From JDK 1.8
        const val REF_CURSOR = 2012
        const val TIME_WITH_TIMEZONE = 2013
        const val TIMESTAMP_WITH_TIMEZONE = 2014

        // From OpenGIS
        const val GEOMETRY = 2015 // TODO: confirm
    }
}
