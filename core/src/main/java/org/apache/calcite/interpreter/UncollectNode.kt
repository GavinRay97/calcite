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

import org.apache.calcite.rel.core.Uncollect

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.Uncollect].
 */
class UncollectNode(compiler: Compiler?, uncollect: Uncollect?) : AbstractSingleNode<Uncollect?>(compiler, uncollect) {
    @Override
    @Throws(InterruptedException::class)
    fun run() {
        var row: Row? = null
        while (source.receive().also { row = it } != null) {
            for (value in row.getValues()) {
                if (value == null) {
                    throw NullPointerException("NULL value for unnest.")
                }
                var i = 1
                if (value is List) {
                    for (o in value) {
                        if (rel.withOrdinality) {
                            sink.send(Row.of(o, i++))
                        } else {
                            sink.send(Row.of(o))
                        }
                    }
                } else if (value is Map) {
                    val map: Map = value
                    for (key in map.keySet()) {
                        if (rel.withOrdinality) {
                            sink.send(Row.of(key, map.get(key), i++))
                        } else {
                            sink.send(Row.of(key, map.get(key)))
                        }
                    }
                } else {
                    throw UnsupportedOperationException(
                        String.format(
                            Locale.ROOT,
                            "Invalid type: %s for unnest.",
                            value.getClass().getCanonicalName()
                        )
                    )
                }
            }
        }
    }
}
