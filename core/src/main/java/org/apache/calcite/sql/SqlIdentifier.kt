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
package org.apache.calcite.sql

import org.apache.calcite.rel.type.DynamicRecordType

/**
 * A `SqlIdentifier` is an identifier, possibly compound.
 */
class SqlIdentifier(
    names: List<String?>,
    @Nullable collation: SqlCollation?,
    pos: SqlParserPos?,
    @Nullable componentPositions: List<SqlParserPos?>?
) : SqlNode(pos) {
    //~ Instance fields --------------------------------------------------------
    /**
     * Array of the components of this compound identifier.
     *
     *
     * The empty string represents the wildcard "*",
     * to distinguish it from a real "*" (presumably specified using quotes).
     *
     *
     * It's convenient to have this member public, and it's convenient to
     * have this member not-final, but it's a shame it's public and not-final.
     * If you assign to this member, please use
     * [.setNames].
     * And yes, we'd like to make identifiers immutable one day.
     */
    var names: ImmutableList<String?>

    /**
     * This identifier's collation (if any).
     */
    @Nullable
    val collation: SqlCollation?

    /**
     * A list of the positions of the components of compound identifiers.
     */
    @Nullable
    protected var componentPositions: ImmutableList<SqlParserPos?>?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a compound identifier, for example `foo.bar`.
     *
     * @param names Parts of the identifier, length  1
     */
    init {
        this.names = ImmutableList.copyOf(names)
        this.collation = collation
        this.componentPositions = if (componentPositions == null) null else ImmutableList.copyOf(componentPositions)
        for (name in names) {
            assert(name != null)
        }
    }

    constructor(names: List<String?>, pos: SqlParserPos?) : this(names, null, pos, null) {}

    /**
     * Creates a simple identifier, for example `foo`, with a
     * collation.
     */
    constructor(
        name: String?,
        @Nullable collation: SqlCollation?,
        pos: SqlParserPos?
    ) : this(ImmutableList.of(name), collation, pos, null) {
    }

    /**
     * Creates a simple identifier, for example `foo`.
     */
    constructor(
        name: String?,
        pos: SqlParserPos?
    ) : this(ImmutableList.of(name), null, pos, null) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.IDENTIFIER

    @Override
    override fun clone(pos: SqlParserPos?): SqlNode {
        return SqlIdentifier(names, collation, pos, componentPositions)
    }

    @Override
    override fun toString(): String {
        return getString(names)
    }

    /**
     * Modifies the components of this identifier and their positions.
     *
     * @param names Names of components
     * @param poses Positions of components
     */
    fun setNames(names: List<String?>?, @Nullable poses: List<SqlParserPos?>?) {
        this.names = ImmutableList.copyOf(names)
        componentPositions = if (poses == null) null else ImmutableList.copyOf(poses)
    }

    /** Returns an identifier that is the same as this except one modified name.
     * Does not modify this identifier.  */
    fun setName(i: Int, name: String): SqlIdentifier {
        return if (!names.get(i).equals(name)) {
            val nameArray: Array<String> = names.toArray(arrayOfNulls<String>(0))
            nameArray[i] = name
            SqlIdentifier(
                ImmutableList.copyOf(nameArray), collation, pos,
                componentPositions
            )
        } else {
            this
        }
    }

    /** Returns an identifier that is the same as this except with a component
     * added at a given position. Does not modify this identifier.  */
    fun add(i: Int, name: String?, pos: SqlParserPos?): SqlIdentifier {
        val names2: List<String> = ArrayList(names)
        names2.add(i, name)
        val pos2: List<SqlParserPos>?
        if (componentPositions == null) {
            pos2 = null
        } else {
            pos2 = ArrayList(componentPositions)
            pos2.add(i, pos)
        }
        return SqlIdentifier(names2, collation, pos, pos2)
    }

    /**
     * Returns the position of the `i`th component of a compound
     * identifier, or the position of the whole identifier if that information
     * is not present.
     *
     * @param i Ordinal of component.
     * @return Position of i'th component
     */
    fun getComponentParserPosition(i: Int): SqlParserPos {
        assert(i >= 0 && i < names.size())
        return if (componentPositions == null) getParserPosition() else componentPositions.get(i)
    }

    /**
     * Copies names and components from another identifier. Does not modify the
     * cross-component parser position.
     *
     * @param other identifier from which to copy
     */
    fun assignNamesFrom(other: SqlIdentifier) {
        setNames(other.names, other.componentPositions)
    }

    /**
     * Creates an identifier which contains only the `ordinal`th
     * component of this compound identifier. It will have the correct
     * [SqlParserPos], provided that detailed position information is
     * available.
     */
    fun getComponent(ordinal: Int): SqlIdentifier {
        return getComponent(ordinal, ordinal + 1)
    }

    fun getComponent(from: Int, to: Int): SqlIdentifier {
        val pos: SqlParserPos
        val pos2: ImmutableList<SqlParserPos?>?
        if (componentPositions == null) {
            pos2 = null
            pos = pos
        } else {
            pos2 = componentPositions.subList(from, to)
            pos = SqlParserPos.sum(pos2)
        }
        return SqlIdentifier(names.subList(from, to), collation, pos, pos2)
    }

    /**
     * Creates an identifier that consists of this identifier plus a name segment.
     * Does not modify this identifier.
     */
    fun plus(name: String?, pos: SqlParserPos): SqlIdentifier {
        val names: ImmutableList<String?> =
            ImmutableList.< String > builder < String ? > ().addAll(names).add(name).build()
        val componentPositions: ImmutableList<SqlParserPos?>?
        val pos2: SqlParserPos
        val thisComponentPositions: ImmutableList<SqlParserPos>? = this.componentPositions
        if (thisComponentPositions != null) {
            val builder: ImmutableList.Builder<SqlParserPos> = ImmutableList.builder()
            componentPositions = builder.addAll(thisComponentPositions).add(pos).build()
            pos2 = SqlParserPos.sum(builder.add(pos).build())
        } else {
            componentPositions = null
            pos2 = pos
        }
        return SqlIdentifier(names, collation, pos2, componentPositions)
    }

    /**
     * Creates an identifier that consists of this identifier plus a wildcard star.
     * Does not modify this identifier.
     */
    fun plusStar(): SqlIdentifier {
        val id = this.plus("*", SqlParserPos.ZERO)
        return SqlIdentifier(
            id.names.stream().map { s -> if (s.equals("*")) "" else s }
                .collect(Util.toImmutableList()),
            null, id.pos, id.componentPositions)
    }

    /** Creates an identifier that consists of all but the last `n`
     * name segments of this one.  */
    fun skipLast(n: Int): SqlIdentifier {
        return getComponent(0, names.size() - n)
    }

    @Override
    override fun unparse(
        writer: SqlWriter?,
        leftPrec: Int,
        rightPrec: Int
    ) {
        SqlUtil.unparseSqlIdentifierSyntax(writer, this, false)
    }

    @Override
    override fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateIdentifier(this, scope)
    }

    @Override
    override fun validateExpr(validator: SqlValidator, scope: SqlValidatorScope?) {
        // First check for builtin functions which don't have parentheses,
        // like "LOCALTIME".
        val call: SqlCall = validator.makeNullaryCall(this)
        if (call != null) {
            validator.validateCall(call, scope)
            return
        }
        validator.validateIdentifier(this, scope)
    }

    @Override
    override fun equalsDeep(@Nullable node: SqlNode?, litmus: Litmus): Boolean {
        if (node !is SqlIdentifier) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node
        if (names.size() !== that.names.size()) {
            return litmus.fail("{} != {}", this, node)
        }
        for (i in 0 until names.size()) {
            if (!names.get(i).equals(that.names.get(i))) {
                return litmus.fail("{} != {}", this, node)
            }
        }
        return litmus.succeed()
    }

    @Override
    override fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Pure
    @Nullable
    fun getCollation(): SqlCollation? {
        return collation
    }

    val simple: String
        get() {
            assert(names.size() === 1)
            return names.get(0)
        }

    /**
     * Returns whether this identifier is a star, such as "*" or "foo.bar.*".
     */
    val isStar: Boolean
        get() = Util.last(names).equals("")

    /**
     * Returns whether this is a simple identifier. "FOO" is simple; "*",
     * "FOO.*" and "FOO.BAR" are not.
     */
    fun isSimple(): Boolean {
        return names.size() === 1 && !isStar
    }

    /**
     * Returns whether the `i`th component of a compound identifier is
     * quoted.
     *
     * @param i Ordinal of component
     * @return Whether i'th component is quoted
     */
    fun isComponentQuoted(i: Int): Boolean {
        return (componentPositions != null
                && componentPositions.get(i).isQuoted())
    }

    @Override
    override fun getMonotonicity(@Nullable scope: SqlValidatorScope): SqlMonotonicity {
        // for "star" column, whether it's static or dynamic return not_monotonic directly.
        if (Util.last(names).equals("") || DynamicRecordType.isDynamicStarColName(Util.last(names))) {
            return SqlMonotonicity.NOT_MONOTONIC
        }
        Objects.requireNonNull(scope, "scope")
        // First check for builtin functions which don't have parentheses,
        // like "LOCALTIME".
        val validator: SqlValidator = scope.getValidator()
        val call: SqlCall = validator.makeNullaryCall(this)
        if (call != null) {
            return call.getMonotonicity(scope)
        }
        val qualified: SqlQualified = scope.fullyQualify(this)
        assert(qualified.namespace != null) { "namespace must not be null in $qualified" }
        val fqId: SqlIdentifier = qualified.identifier
        return qualified.namespace.resolve().getMonotonicity(Util.last(fqId.names))
    }

    companion object {
        /** An identifier for star, "*".
         *
         * @see SqlNodeList.SINGLETON_STAR
         */
        val STAR = star(SqlParserPos.ZERO)

        /** Creates an identifier that is a singleton wildcard star.  */
        fun star(pos: SqlParserPos?): SqlIdentifier {
            return star(ImmutableList.of(""), pos, ImmutableList.of(pos))
        }

        /** Creates an identifier that ends in a wildcard star.  */
        fun star(
            names: List<String?>?, pos: SqlParserPos?,
            componentPositions: List<SqlParserPos?>?
        ): SqlIdentifier {
            return SqlIdentifier(
                Util.transform(names) { s -> if (s.equals("*")) "" else s }, null, pos,
                componentPositions
            )
        }

        /** Converts a list of strings to a qualified identifier.  */
        fun getString(names: List<String?>?): String {
            return Util.sepList(toStar(names), ".")
        }

        /** Converts empty strings in a list of names to stars.  */
        fun toStar(names: List<String?>?): List<String> {
            return Util.transform(names) { s -> if (s.equals("")) "*" else s }
        }

        /** Returns the simple names in a list of identifiers.
         * Assumes that the list consists of are not-null, simple identifiers.  */
        fun simpleNames(list: List<SqlNode?>?): List<String> {
            return Util.transform(list) { n -> (n as SqlIdentifier).simple }
        }

        /** Returns the simple names in a iterable of identifiers.
         * Assumes that the iterable consists of not-null, simple identifiers.  */
        fun simpleNames(list: Iterable<SqlNode?>?): Iterable<String> {
            return Util.transform(list) { n -> (n as SqlIdentifier).simple }
        }
    }
}
