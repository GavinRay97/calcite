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
package org.apache.calcite.server

import org.apache.calcite.jdbc.CalcitePrepare

/** Abstract implementation of [org.apache.calcite.server.DdlExecutor].  */
class DdlExecutorImpl
/** Creates a DdlExecutorImpl.
 * Protected only to allow sub-classing;
 * use a singleton instance where possible.  */
protected constructor() : DdlExecutor, ReflectiveVisitor {
    /** Dispatches calls to the appropriate method based on the type of the
     * first argument.  */
    @SuppressWarnings(["method.invocation.invalid", "argument.type.incompatible"])
    private val dispatcher: ReflectUtil.MethodDispatcher<Void> = ReflectUtil.createMethodDispatcher(
        Void.TYPE, this, "execute",
        SqlNode::class.java, CalcitePrepare.Context::class.java
    )

    @Override
    override fun executeDdl(
        context: CalcitePrepare.Context?,
        node: SqlNode?
    ) {
        dispatcher.invoke(node, context)
    }

    /** Template for methods that execute DDL commands.
     *
     *
     * The base implementation throws [UnsupportedOperationException]
     * because a [SqlNode] is not DDL, but overloaded methods such as
     * `public void execute(SqlCreateFoo, CalcitePrepare.Context)` are
     * called via reflection.  */
    fun execute(node: SqlNode, context: CalcitePrepare.Context?) {
        throw UnsupportedOperationException("DDL not supported: $node")
    }
}
