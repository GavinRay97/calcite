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
package org.apache.calcite.rel.type

/**
 * Describes a policy for resolving fields in record types.
 *
 *
 * The usual value is [.FULLY_QUALIFIED].
 *
 *
 * A field whose record type is labeled [.PEEK_FIELDS] can be omitted.
 * In Phoenix, column families are represented by fields like this.
 * [.PEEK_FIELDS_DEFAULT] is similar, but represents the default column
 * family, so it will win in the event of a tie.
 *
 *
 * SQL usually disallows a record type. For instance,
 *
 * <blockquote><pre>SELECT address.zip FROM Emp AS e</pre></blockquote>
 *
 *
 * is disallowed because `address` "looks like" a table alias. You'd
 * have to write
 *
 * <blockquote><pre>SELECT e.address.zip FROM Emp AS e</pre></blockquote>
 *
 *
 * But if a table has one or more columns that are record-typed and are
 * labeled [.PEEK_FIELDS] or [.PEEK_FIELDS_DEFAULT] we suspend that
 * rule and would allow `address.zip`.
 *
 *
 * If there are multiple matches, we choose the one that is:
 *
 *  1. Shorter. If you write `zipcode`, `address.zipcode` will
 * be preferred over `product.supplier.zipcode`.
 *  1. Uses as little skipping as possible. A match that is fully-qualified
 * will beat one that uses `PEEK_FIELDS_DEFAULT` at some point, which
 * will beat one that uses `PEEK_FIELDS` at some point.
 *
 */
enum class StructKind {
    /** This is not a structured type.  */
    NONE,

    /** This is a traditional structured type, where each field must be
     * referenced explicitly.
     *
     *
     * Also, when referencing a struct column, you
     * need to qualify it with the table alias, per standard SQL. For instance,
     * `SELECT c.address.zipcode FROM customer AS c`
     * is valid but
     * `SELECT address.zipcode FROM customer`
     * it not valid.
     */
    FULLY_QUALIFIED,

    /** As [.PEEK_FIELDS], but takes priority if another struct-typed
     * field also has a field of the name being sought.
     *
     *
     * In Phoenix, only one of a table's columns is labeled
     * `PEEK_FIELDS_DEFAULT` - the default column family - but in principle
     * there could be more than one.  */
    PEEK_FIELDS_DEFAULT,

    /** If a field has this type, you can see its fields without qualifying them
     * with the name of this field.
     *
     *
     * For example, if `address` is labeled `PEEK_FIELDS`, you
     * could write `zipcode` as shorthand for `address.zipcode`.  */
    PEEK_FIELDS,

    /** As [.PEEK_FIELDS], but fields are not expanded in "SELECT *".
     *
     *
     * Used in Flink, not Phoenix.  */
    PEEK_FIELDS_NO_EXPAND
}
