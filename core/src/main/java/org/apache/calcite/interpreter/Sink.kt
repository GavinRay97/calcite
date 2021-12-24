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
package org.apache.calcite.interpreter

import org.apache.calcite.linq4j.Enumerable

/**
 * Sink to which to send rows.
 *
 *
 * Corresponds to an output of a relational expression.
 */
interface Sink {
    @Throws(InterruptedException::class)
    fun send(row: Row?)

    @Throws(InterruptedException::class)
    fun end()

    /** This method is temporary. It will be removed without notice.  */
    @Deprecated
    @Throws(InterruptedException::class)
    fun setSourceEnumerable(enumerable: Enumerable<Row?>?)
}
