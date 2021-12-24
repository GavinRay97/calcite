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
package org.apache.calcite.sql.validate

import org.apache.calcite.config.CalciteSystemProperty

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Calcite code.
/**
 * Exception thrown while validating a SQL statement.
 *
 *
 * Unlike [org.apache.calcite.runtime.CalciteException], this is a
 * checked exception, which reminds code authors to wrap it in another exception
 * containing the line/column context.
 */
class SqlValidatorException @SuppressWarnings(["argument.type.incompatible", "method.invocation.invalid"]) constructor(
    message: String?,
    cause: Throwable?
) : Exception(message, cause), CalciteValidatorException {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new SqlValidatorException object.
     *
     * @param message error message
     * @param cause   underlying cause
     */
    init {

        // TODO: see note in CalciteException constructor
        LOGGER.trace("SqlValidatorException", this)
        if (CalciteSystemProperty.DEBUG.value()) {
            LOGGER.error(toString())
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val LOGGER: Logger = LoggerFactory.getLogger("org.apache.calcite.runtime.CalciteException")
        const val serialVersionUID = -831683113957131387L
    }
}
