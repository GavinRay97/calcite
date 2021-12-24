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
package org.apache.calcite.util

import org.apache.calcite.DataContext

/**
 * Built-in methods.
 */
enum class BuiltInMethod(@Nullable method: Method?, @Nullable constructor: Constructor?, @Nullable field: Field?) {
    QUERYABLE_SELECT(
        Queryable::class.java,
        "select",
        FunctionExpression::class.java
    ),
    QUERYABLE_AS_ENUMERABLE(Queryable::class.java, "asEnumerable"), QUERYABLE_TABLE_AS_QUERYABLE(
        QueryableTable::class.java, "asQueryable",
        QueryProvider::class.java, SchemaPlus::class.java, String::class.java
    ),
    AS_QUERYABLE(Enumerable::class.java, "asQueryable"), ABSTRACT_ENUMERABLE_CTOR(
        AbstractEnumerable::class.java
    ),
    INTO(ExtendedEnumerable::class.java, "into", Collection::class.java), REMOVE_ALL(
        ExtendedEnumerable::class.java, "removeAll", Collection::class.java
    ),
    SCHEMA_GET_SUB_SCHEMA(Schema::class.java, "getSubSchema", String::class.java), SCHEMA_GET_TABLE(
        Schema::class.java, "getTable", String::class.java
    ),
    SCHEMA_PLUS_UNWRAP(SchemaPlus::class.java, "unwrap", Class::class.java), SCHEMAS_ENUMERABLE_SCANNABLE(
        Schemas::class.java, "enumerable",
        ScannableTable::class.java, DataContext::class.java
    ),
    SCHEMAS_ENUMERABLE_FILTERABLE(
        Schemas::class.java, "enumerable",
        FilterableTable::class.java, DataContext::class.java
    ),
    SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE(
        Schemas::class.java, "enumerable",
        ProjectableFilterableTable::class.java, DataContext::class.java
    ),
    SCHEMAS_QUERYABLE(
        Schemas::class.java, "queryable", DataContext::class.java,
        SchemaPlus::class.java, Class::class.java, String::class.java
    ),
    REFLECTIVE_SCHEMA_GET_TARGET(ReflectiveSchema::class.java, "getTarget"), DATA_CONTEXT_GET(
        DataContext::class.java, "get", String::class.java
    ),
    DATA_CONTEXT_GET_ROOT_SCHEMA(DataContext::class.java, "getRootSchema"), JDBC_SCHEMA_DATA_SOURCE(
        JdbcSchema::class.java, "getDataSource"
    ),
    ROW_VALUE(Row::class.java, "getObject", Int::class.javaPrimitiveType), ROW_AS_COPY(
        Row::class.java, "asCopy", Array<Object>::class.java
    ),
    RESULT_SET_ENUMERABLE_SET_TIMEOUT(
        ResultSetEnumerable::class.java, "setTimeout",
        DataContext::class.java
    ),
    RESULT_SET_ENUMERABLE_OF(
        ResultSetEnumerable::class.java, "of", DataSource::class.java,
        String::class.java, Function1::class.java
    ),
    RESULT_SET_ENUMERABLE_OF_PREPARED(
        ResultSetEnumerable::class.java, "of",
        DataSource::class.java, String::class.java, Function1::class.java,
        ResultSetEnumerable.PreparedStatementEnricher::class.java
    ),
    CREATE_ENRICHER(
        ResultSetEnumerable::class.java, "createEnricher", Array<Integer>::class.java,
        DataContext::class.java
    ),
    HASH_JOIN(
        ExtendedEnumerable::class.java, "hashJoin", Enumerable::class.java,
        Function1::class.java,
        Function1::class.java, Function2::class.java, EqualityComparer::class.java,
        Boolean::class.javaPrimitiveType, Boolean::class.javaPrimitiveType, Predicate2::class.java
    ),
    MATCH(
        Enumerables::class.java, "match", Enumerable::class.java, Function1::class.java,
        Matcher::class.java, Enumerables.Emitter::class.java, Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    PATTERN_BUILDER(
        Utilities::class.java, "patternBuilder"
    ),
    PATTERN_BUILDER_SYMBOL(Pattern.PatternBuilder::class.java, "symbol", String::class.java), PATTERN_BUILDER_SEQ(
        Pattern.PatternBuilder::class.java, "seq"
    ),
    PATTERN_BUILDER_BUILD(Pattern.PatternBuilder::class.java, "build"), PATTERN_TO_AUTOMATON(
        Pattern.PatternBuilder::class.java, "automaton"
    ),
    MATCHER_BUILDER(Matcher::class.java, "builder", Automaton::class.java), MATCHER_BUILDER_ADD(
        Matcher.Builder::class.java, "add", String::class.java,
        Predicate::class.java
    ),
    MATCHER_BUILDER_BUILD(Matcher.Builder::class.java, "build"), MATCH_UTILS_LAST_WITH_SYMBOL(
        MatchUtils::class.java, "lastWithSymbol", String::class.java,
        List::class.java, List::class.java, Int::class.javaPrimitiveType
    ),
    EMITTER_EMIT(
        Enumerables.Emitter::class.java, "emit", List::class.java, List::class.java,
        List::class.java, Int::class.javaPrimitiveType, Consumer::class.java
    ),
    MERGE_JOIN(
        EnumerableDefaults::class.java,
        "mergeJoin",
        Enumerable::class.java,
        Enumerable::class.java,
        Function1::class.java,
        Function1::class.java,
        Predicate2::class.java,
        Function2::class.java,
        JoinType::class.java,
        Comparator::class.java
    ),
    SLICE0(Enumerables::class.java, "slice0", Enumerable::class.java), SEMI_JOIN(
        EnumerableDefaults::class.java, "semiJoin", Enumerable::class.java,
        Enumerable::class.java, Function1::class.java, Function1::class.java,
        EqualityComparer::class.java, Predicate2::class.java
    ),
    ANTI_JOIN(
        EnumerableDefaults::class.java, "antiJoin", Enumerable::class.java,
        Enumerable::class.java, Function1::class.java, Function1::class.java,
        EqualityComparer::class.java, Predicate2::class.java
    ),
    NESTED_LOOP_JOIN(
        EnumerableDefaults::class.java, "nestedLoopJoin", Enumerable::class.java,
        Enumerable::class.java, Predicate2::class.java, Function2::class.java, JoinType::class.java
    ),
    CORRELATE_JOIN(
        ExtendedEnumerable::class.java, "correlateJoin",
        JoinType::class.java, Function1::class.java, Function2::class.java
    ),
    CORRELATE_BATCH_JOIN(
        EnumerableDefaults::class.java, "correlateBatchJoin",
        JoinType::class.java, Enumerable::class.java, Function1::class.java, Function2::class.java,
        Predicate2::class.java, Int::class.javaPrimitiveType
    ),
    SELECT(ExtendedEnumerable::class.java, "select", Function1::class.java), SELECT2(
        ExtendedEnumerable::class.java, "select", Function2::class.java
    ),
    SELECT_MANY(ExtendedEnumerable::class.java, "selectMany", Function1::class.java), WHERE(
        ExtendedEnumerable::class.java, "where", Predicate1::class.java
    ),
    WHERE2(ExtendedEnumerable::class.java, "where", Predicate2::class.java), DISTINCT(
        ExtendedEnumerable::class.java, "distinct"
    ),
    DISTINCT2(ExtendedEnumerable::class.java, "distinct", EqualityComparer::class.java), SORTED_GROUP_BY(
        ExtendedEnumerable::class.java, "sortedGroupBy", Function1::class.java,
        Function0::class.java, Function2::class.java, Function2::class.java, Comparator::class.java
    ),
    GROUP_BY(
        ExtendedEnumerable::class.java, "groupBy", Function1::class.java
    ),
    GROUP_BY2(
        ExtendedEnumerable::class.java, "groupBy", Function1::class.java,
        Function0::class.java, Function2::class.java, Function2::class.java
    ),
    GROUP_BY_MULTIPLE(
        EnumerableDefaults::class.java, "groupByMultiple",
        Enumerable::class.java, List::class.java, Function0::class.java, Function2::class.java,
        Function2::class.java
    ),
    AGGREGATE(
        ExtendedEnumerable::class.java, "aggregate", Object::class.java,
        Function2::class.java, Function1::class.java
    ),
    ORDER_BY(
        ExtendedEnumerable::class.java, "orderBy", Function1::class.java,
        Comparator::class.java
    ),
    ORDER_BY_WITH_FETCH_AND_OFFSET(
        EnumerableDefaults::class.java, "orderBy", Enumerable::class.java,
        Function1::class.java, Comparator::class.java, Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    UNION(
        ExtendedEnumerable::class.java, "union", Enumerable::class.java
    ),
    CONCAT(ExtendedEnumerable::class.java, "concat", Enumerable::class.java), REPEAT_UNION(
        EnumerableDefaults::class.java,
        "repeatUnion",
        Enumerable::class.java,
        Enumerable::class.java,
        Int::class.javaPrimitiveType,
        Boolean::class.javaPrimitiveType,
        EqualityComparer::class.java
    ),
    MERGE_UNION(
        EnumerableDefaults::class.java, "mergeUnion", List::class.java, Function1::class.java,
        Comparator::class.java, Boolean::class.javaPrimitiveType, EqualityComparer::class.java
    ),
    LAZY_COLLECTION_SPOOL(
        EnumerableDefaults::class.java, "lazyCollectionSpool", Collection::class.java,
        Enumerable::class.java
    ),
    INTERSECT(
        ExtendedEnumerable::class.java,
        "intersect",
        Enumerable::class.java,
        Boolean::class.javaPrimitiveType
    ),
    EXCEPT(
        ExtendedEnumerable::class.java, "except", Enumerable::class.java, Boolean::class.javaPrimitiveType
    ),
    SKIP(
        ExtendedEnumerable::class.java, "skip", Int::class.javaPrimitiveType
    ),
    TAKE(ExtendedEnumerable::class.java, "take", Int::class.javaPrimitiveType), SINGLETON_ENUMERABLE(
        Linq4j::class.java, "singletonEnumerable", Object::class.java
    ),
    EMPTY_ENUMERABLE(Linq4j::class.java, "emptyEnumerable"), NULLS_COMPARATOR(
        Functions::class.java, "nullsComparator", Boolean::class.javaPrimitiveType,
        Boolean::class.javaPrimitiveType
    ),
    NULLS_COMPARATOR2(
        Functions::class.java, "nullsComparator", Boolean::class.javaPrimitiveType,
        Boolean::class.javaPrimitiveType, Comparator::class.java
    ),
    ARRAY_COMPARER(Functions::class.java, "arrayComparer"), FUNCTION0_APPLY(
        Function0::class.java, "apply"
    ),
    FUNCTION1_APPLY(Function1::class.java, "apply", Object::class.java), ARRAYS_AS_LIST(
        Arrays::class.java, "asList", Array<Object>::class.java
    ),
    ARRAY(SqlFunctions::class.java, "array", Array<Object>::class.java), FLAT_PRODUCT(
        SqlFunctions::class.java, "flatProduct", IntArray::class.java, Boolean::class.javaPrimitiveType,
        Array<FlatProductInputType>::class.java
    ),
    FLAT_LIST(SqlFunctions::class.java, "flatList"), LIST_N(
        FlatLists::class.java,
        "copyOf",
        Array<Comparable>::class.java
    ),
    LIST2(
        FlatLists::class.java, "of", Object::class.java, Object::class.java
    ),
    LIST3(FlatLists::class.java, "of", Object::class.java, Object::class.java, Object::class.java), LIST4(
        FlatLists::class.java, "of", Object::class.java, Object::class.java, Object::class.java,
        Object::class.java
    ),
    LIST5(
        FlatLists::class.java, "of", Object::class.java, Object::class.java, Object::class.java,
        Object::class.java, Object::class.java
    ),
    LIST6(
        FlatLists::class.java, "of", Object::class.java, Object::class.java, Object::class.java,
        Object::class.java, Object::class.java, Object::class.java
    ),
    COMPARABLE_EMPTY_LIST(FlatLists::class.java, "COMPARABLE_EMPTY_LIST", true), IDENTITY_COMPARER(
        Functions::class.java, "identityComparer"
    ),
    IDENTITY_SELECTOR(Functions::class.java, "identitySelector"), AS_ENUMERABLE(
        Linq4j::class.java, "asEnumerable", Array<Object>::class.java
    ),
    AS_ENUMERABLE2(Linq4j::class.java, "asEnumerable", Iterable::class.java), ENUMERABLE_TO_LIST(
        ExtendedEnumerable::class.java, "toList"
    ),
    AS_LIST(Primitive::class.java, "asList", Object::class.java), MEMORY_GET0(
        MemoryFactory.Memory::class.java, "get"
    ),
    MEMORY_GET1(MemoryFactory.Memory::class.java, "get", Int::class.javaPrimitiveType), ENUMERATOR_CURRENT(
        Enumerator::class.java, "current"
    ),
    ENUMERATOR_MOVE_NEXT(Enumerator::class.java, "moveNext"), ENUMERATOR_CLOSE(
        Enumerator::class.java, "close"
    ),
    ENUMERATOR_RESET(Enumerator::class.java, "reset"), ENUMERABLE_ENUMERATOR(
        Enumerable::class.java, "enumerator"
    ),
    ENUMERABLE_FOREACH(Enumerable::class.java, "foreach", Function1::class.java), ITERABLE_FOR_EACH(
        Iterable::class.java, "forEach", Consumer::class.java
    ),
    FUNCTION_APPLY(Function::class.java, "apply", Object::class.java), PREDICATE_TEST(
        Predicate::class.java, "test", Object::class.java
    ),
    BI_PREDICATE_TEST(BiPredicate::class.java, "test", Object::class.java, Object::class.java), CONSUMER_ACCEPT(
        Consumer::class.java, "accept", Object::class.java
    ),
    TYPED_GET_ELEMENT_TYPE(ArrayBindable::class.java, "getElementType"), BINDABLE_BIND(
        Bindable::class.java, "bind", DataContext::class.java
    ),
    RESULT_SET_GET_DATE2(
        ResultSet::class.java,
        "getDate",
        Int::class.javaPrimitiveType,
        Calendar::class.java
    ),
    RESULT_SET_GET_TIME2(
        ResultSet::class.java, "getTime", Int::class.javaPrimitiveType, Calendar::class.java
    ),
    RESULT_SET_GET_TIMESTAMP2(
        ResultSet::class.java, "getTimestamp", Int::class.javaPrimitiveType,
        Calendar::class.java
    ),
    TIME_ZONE_GET_OFFSET(TimeZone::class.java, "getOffset", Long::class.javaPrimitiveType), LONG_VALUE(
        Number::class.java, "longValue"
    ),
    COMPARATOR_COMPARE(
        Comparator::class.java,
        "compare",
        Object::class.java,
        Object::class.java
    ),
    COLLECTIONS_REVERSE_ORDER(
        Collections::class.java, "reverseOrder"
    ),
    COLLECTIONS_EMPTY_LIST(Collections::class.java, "emptyList"), COLLECTIONS_SINGLETON_LIST(
        Collections::class.java, "singletonList", Object::class.java
    ),
    COLLECTION_SIZE(Collection::class.java, "size"), MAP_CLEAR(
        Map::class.java, "clear"
    ),
    MAP_GET(Map::class.java, "get", Object::class.java), MAP_GET_OR_DEFAULT(
        Map::class.java,
        "getOrDefault",
        Object::class.java,
        Object::class.java
    ),
    MAP_PUT(
        Map::class.java, "put", Object::class.java, Object::class.java
    ),
    COLLECTION_ADD(Collection::class.java, "add", Object::class.java), COLLECTION_ADDALL(
        Collection::class.java, "addAll", Collection::class.java
    ),
    COLLECTION_RETAIN_ALL(Collection::class.java, "retainAll", Collection::class.java), LIST_GET(
        List::class.java, "get", Int::class.javaPrimitiveType
    ),
    ITERATOR_HAS_NEXT(Iterator::class.java, "hasNext"), ITERATOR_NEXT(
        Iterator::class.java, "next"
    ),
    MATH_MAX(Math::class.java, "max", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType), MATH_MIN(
        Math::class.java, "min", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    SORTED_MULTI_MAP_PUT_MULTI(
        SortedMultiMap::class.java, "putMulti", Object::class.java,
        Object::class.java
    ),
    SORTED_MULTI_MAP_ARRAYS(SortedMultiMap::class.java, "arrays", Comparator::class.java), SORTED_MULTI_MAP_SINGLETON(
        SortedMultiMap::class.java, "singletonArrayIterator",
        Comparator::class.java, List::class.java
    ),
    BINARY_SEARCH5_LOWER(
        BinarySearch::class.java, "lowerBound", Array<Object>::class.java,
        Object::class.java, Int::class.javaPrimitiveType, Int::class.javaPrimitiveType, Comparator::class.java
    ),
    BINARY_SEARCH5_UPPER(
        BinarySearch::class.java, "upperBound", Array<Object>::class.java,
        Object::class.java, Int::class.javaPrimitiveType, Int::class.javaPrimitiveType, Comparator::class.java
    ),
    BINARY_SEARCH6_LOWER(
        BinarySearch::class.java,
        "lowerBound",
        Array<Object>::class.java,
        Object::class.java,
        Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType,
        Function1::class.java,
        Comparator::class.java
    ),
    BINARY_SEARCH6_UPPER(
        BinarySearch::class.java,
        "upperBound",
        Array<Object>::class.java,
        Object::class.java,
        Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType,
        Function1::class.java,
        Comparator::class.java
    ),
    ARRAY_ITEM(
        SqlFunctions::class.java, "arrayItemOptional", List::class.java, Int::class.javaPrimitiveType
    ),
    MAP_ITEM(
        SqlFunctions::class.java, "mapItemOptional", Map::class.java, Object::class.java
    ),
    ANY_ITEM(SqlFunctions::class.java, "itemOptional", Object::class.java, Object::class.java), UPPER(
        SqlFunctions::class.java, "upper", String::class.java
    ),
    LOWER(SqlFunctions::class.java, "lower", String::class.java), ASCII(
        SqlFunctions::class.java, "ascii", String::class.java
    ),
    REPEAT(SqlFunctions::class.java, "repeat", String::class.java, Int::class.javaPrimitiveType), SPACE(
        SqlFunctions::class.java, "space", Int::class.javaPrimitiveType
    ),
    SOUNDEX(SqlFunctions::class.java, "soundex", String::class.java), STRCMP(
        SqlFunctions::class.java, "strcmp", String::class.java, String::class.java
    ),
    DIFFERENCE(SqlFunctions::class.java, "difference", String::class.java, String::class.java), REVERSE(
        SqlFunctions::class.java, "reverse", String::class.java
    ),
    LEFT(SqlFunctions::class.java, "left", String::class.java, Int::class.javaPrimitiveType), RIGHT(
        SqlFunctions::class.java, "right", String::class.java, Int::class.javaPrimitiveType
    ),
    TO_BASE64(SqlFunctions::class.java, "toBase64", String::class.java), FROM_BASE64(
        SqlFunctions::class.java, "fromBase64", String::class.java
    ),
    MD5(SqlFunctions::class.java, "md5", String::class.java), SHA1(
        SqlFunctions::class.java, "sha1", String::class.java
    ),
    THROW_UNLESS(
        SqlFunctions::class.java,
        "throwUnless",
        Boolean::class.javaPrimitiveType,
        String::class.java
    ),
    COMPRESS(
        CompressionFunctions::class.java, "compress", String::class.java
    ),
    EXTRACT_VALUE(XmlFunctions::class.java, "extractValue", String::class.java, String::class.java), XML_TRANSFORM(
        XmlFunctions::class.java, "xmlTransform", String::class.java, String::class.java
    ),
    EXTRACT_XML(
        XmlFunctions::class.java,
        "extractXml",
        String::class.java,
        String::class.java,
        String::class.java
    ),
    EXISTS_NODE(
        XmlFunctions::class.java, "existsNode", String::class.java, String::class.java, String::class.java
    ),
    JSONIZE(
        JsonFunctions::class.java, "jsonize", Object::class.java
    ),
    DEJSONIZE(JsonFunctions::class.java, "dejsonize", String::class.java), JSON_VALUE_EXPRESSION(
        JsonFunctions::class.java, "jsonValueExpression",
        String::class.java
    ),
    JSON_API_COMMON_SYNTAX(
        JsonFunctions::class.java, "jsonApiCommonSyntax",
        String::class.java, String::class.java
    ),
    JSON_EXISTS(JsonFunctions::class.java, "jsonExists", String::class.java, String::class.java), JSON_VALUE(
        JsonFunctions::class.java, "jsonValue", String::class.java, String::class.java,
        SqlJsonValueEmptyOrErrorBehavior::class.java, Object::class.java,
        SqlJsonValueEmptyOrErrorBehavior::class.java, Object::class.java
    ),
    JSON_QUERY(
        JsonFunctions::class.java, "jsonQuery", String::class.java,
        String::class.java,
        SqlJsonQueryWrapperBehavior::class.java,
        SqlJsonQueryEmptyOrErrorBehavior::class.java,
        SqlJsonQueryEmptyOrErrorBehavior::class.java
    ),
    JSON_OBJECT(
        JsonFunctions::class.java, "jsonObject",
        SqlJsonConstructorNullClause::class.java
    ),
    JSON_TYPE(JsonFunctions::class.java, "jsonType", String::class.java), JSON_DEPTH(
        JsonFunctions::class.java, "jsonDepth", String::class.java
    ),
    JSON_KEYS(JsonFunctions::class.java, "jsonKeys", String::class.java), JSON_PRETTY(
        JsonFunctions::class.java, "jsonPretty", String::class.java
    ),
    JSON_LENGTH(JsonFunctions::class.java, "jsonLength", String::class.java), JSON_REMOVE(
        JsonFunctions::class.java, "jsonRemove", String::class.java
    ),
    JSON_STORAGE_SIZE(JsonFunctions::class.java, "jsonStorageSize", String::class.java), JSON_OBJECTAGG_ADD(
        JsonFunctions::class.java, "jsonObjectAggAdd", Map::class.java,
        String::class.java, Object::class.java, SqlJsonConstructorNullClause::class.java
    ),
    JSON_ARRAY(
        JsonFunctions::class.java, "jsonArray",
        SqlJsonConstructorNullClause::class.java
    ),
    JSON_ARRAYAGG_ADD(
        JsonFunctions::class.java, "jsonArrayAggAdd",
        List::class.java, Object::class.java, SqlJsonConstructorNullClause::class.java
    ),
    IS_JSON_VALUE(JsonFunctions::class.java, "isJsonValue", String::class.java), IS_JSON_OBJECT(
        JsonFunctions::class.java, "isJsonObject", String::class.java
    ),
    IS_JSON_ARRAY(JsonFunctions::class.java, "isJsonArray", String::class.java), IS_JSON_SCALAR(
        JsonFunctions::class.java, "isJsonScalar", String::class.java
    ),
    ST_GEOM_FROM_TEXT(GeoFunctions::class.java, "ST_GeomFromText", String::class.java), INITCAP(
        SqlFunctions::class.java, "initcap", String::class.java
    ),
    SUBSTRING(
        SqlFunctions::class.java, "substring", String::class.java, Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType
    ),
    OCTET_LENGTH(SqlFunctions::class.java, "octetLength", ByteString::class.java), CHAR_LENGTH(
        SqlFunctions::class.java, "charLength", String::class.java
    ),
    STRING_CONCAT(SqlFunctions::class.java, "concat", String::class.java, String::class.java), MULTI_STRING_CONCAT(
        SqlFunctions::class.java, "concatMulti", Array<String>::class.java
    ),
    FLOOR_DIV(
        DateTimeUtils::class.java,
        "floorDiv",
        Long::class.javaPrimitiveType,
        Long::class.javaPrimitiveType
    ),
    FLOOR_MOD(
        DateTimeUtils::class.java, "floorMod", Long::class.javaPrimitiveType, Long::class.javaPrimitiveType
    ),
    ADD_MONTHS(
        SqlFunctions::class.java, "addMonths", Long::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    ADD_MONTHS_INT(
        SqlFunctions::class.java, "addMonths", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    SUBTRACT_MONTHS(
        SqlFunctions::class.java, "subtractMonths", Long::class.javaPrimitiveType,
        Long::class.javaPrimitiveType
    ),
    FLOOR(SqlFunctions::class.java, "floor", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType), CEIL(
        SqlFunctions::class.java, "ceil", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    COSH(
        SqlFunctions::class.java, "cosh", Long::class.javaPrimitiveType
    ),
    OVERLAY(
        SqlFunctions::class.java,
        "overlay",
        String::class.java,
        String::class.java,
        Int::class.javaPrimitiveType
    ),
    OVERLAY3(
        SqlFunctions::class.java, "overlay", String::class.java, String::class.java, Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType
    ),
    POSITION(SqlFunctions::class.java, "position", String::class.java, String::class.java), RAND(
        RandomFunction::class.java, "rand"
    ),
    RAND_SEED(RandomFunction::class.java, "randSeed", Int::class.javaPrimitiveType), RAND_INTEGER(
        RandomFunction::class.java, "randInteger", Int::class.javaPrimitiveType
    ),
    RAND_INTEGER_SEED(
        RandomFunction::class.java, "randIntegerSeed", Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType
    ),
    TANH(SqlFunctions::class.java, "tanh", Long::class.javaPrimitiveType), SINH(
        SqlFunctions::class.java, "sinh", Long::class.javaPrimitiveType
    ),
    TRUNCATE(SqlFunctions::class.java, "truncate", String::class.java, Int::class.javaPrimitiveType), TRUNCATE_OR_PAD(
        SqlFunctions::class.java, "truncateOrPad", String::class.java, Int::class.javaPrimitiveType
    ),
    TRIM(
        SqlFunctions::class.java,
        "trim",
        Boolean::class.javaPrimitiveType,
        Boolean::class.javaPrimitiveType,
        String::class.java,
        String::class.java,
        Boolean::class.javaPrimitiveType
    ),
    REPLACE(
        SqlFunctions::class.java, "replace", String::class.java, String::class.java,
        String::class.java
    ),
    TRANSLATE3(
        SqlFunctions::class.java,
        "translate3",
        String::class.java,
        String::class.java,
        String::class.java
    ),
    LTRIM(
        SqlFunctions::class.java, "ltrim", String::class.java
    ),
    RTRIM(SqlFunctions::class.java, "rtrim", String::class.java), LIKE(
        SqlFunctions::class.java, "like", String::class.java, String::class.java
    ),
    ILIKE(SqlFunctions::class.java, "ilike", String::class.java, String::class.java), RLIKE(
        SqlFunctions::class.java, "rlike", String::class.java, String::class.java
    ),
    SIMILAR(SqlFunctions::class.java, "similar", String::class.java, String::class.java), POSIX_REGEX(
        SqlFunctions::class.java, "posixRegex", String::class.java, String::class.java, Boolean::class.javaPrimitiveType
    ),
    REGEXP_REPLACE3(
        SqlFunctions::class.java, "regexpReplace", String::class.java,
        String::class.java, String::class.java
    ),
    REGEXP_REPLACE4(
        SqlFunctions::class.java, "regexpReplace", String::class.java,
        String::class.java, String::class.java, Int::class.javaPrimitiveType
    ),
    REGEXP_REPLACE5(
        SqlFunctions::class.java, "regexpReplace", String::class.java,
        String::class.java, String::class.java, Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    REGEXP_REPLACE6(
        SqlFunctions::class.java,
        "regexpReplace",
        String::class.java,
        String::class.java,
        String::class.java,
        Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType,
        String::class.java
    ),
    IS_TRUE(
        SqlFunctions::class.java, "isTrue", Boolean::class.java
    ),
    IS_NOT_FALSE(SqlFunctions::class.java, "isNotFalse", Boolean::class.java), NOT(
        SqlFunctions::class.java, "not", Boolean::class.java
    ),
    LESSER(SqlFunctions::class.java, "lesser", Comparable::class.java, Comparable::class.java), GREATER(
        SqlFunctions::class.java, "greater", Comparable::class.java, Comparable::class.java
    ),
    BIT_AND(SqlFunctions::class.java, "bitAnd", Long::class.javaPrimitiveType, Long::class.javaPrimitiveType), BIT_OR(
        SqlFunctions::class.java, "bitOr", Long::class.javaPrimitiveType, Long::class.javaPrimitiveType
    ),
    BIT_XOR(
        SqlFunctions::class.java, "bitXor", Long::class.javaPrimitiveType, Long::class.javaPrimitiveType
    ),
    MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION(
        ModifiableTable::class.java,
        "getModifiableCollection"
    ),
    SCANNABLE_TABLE_SCAN(ScannableTable::class.java, "scan", DataContext::class.java), STRING_TO_BOOLEAN(
        SqlFunctions::class.java, "toBoolean", String::class.java
    ),
    INTERNAL_TO_DATE(SqlFunctions::class.java, "internalToDate", Int::class.javaPrimitiveType), INTERNAL_TO_TIME(
        SqlFunctions::class.java, "internalToTime", Int::class.javaPrimitiveType
    ),
    INTERNAL_TO_TIMESTAMP(
        SqlFunctions::class.java,
        "internalToTimestamp",
        Long::class.javaPrimitiveType
    ),
    STRING_TO_DATE(
        DateTimeUtils::class.java, "dateStringToUnixDate", String::class.java
    ),
    STRING_TO_TIME(DateTimeUtils::class.java, "timeStringToUnixDate", String::class.java), STRING_TO_TIMESTAMP(
        DateTimeUtils::class.java, "timestampStringToUnixDate", String::class.java
    ),
    STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(
        SqlFunctions::class.java, "toTimeWithLocalTimeZone",
        String::class.java
    ),
    TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(
        SqlFunctions::class.java, "toTimeWithLocalTimeZone",
        String::class.java, TimeZone::class.java
    ),
    STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(
        SqlFunctions::class.java, "toTimestampWithLocalTimeZone",
        String::class.java
    ),
    TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(
        SqlFunctions::class.java,
        "toTimestampWithLocalTimeZone", String::class.java, TimeZone::class.java
    ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIME(
        SqlFunctions::class.java, "timeWithLocalTimeZoneToTime",
        Int::class.javaPrimitiveType, TimeZone::class.java
    ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(
        SqlFunctions::class.java, "timeWithLocalTimeZoneToTimestamp",
        String::class.java, Int::class.javaPrimitiveType, TimeZone::class.java
    ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(
        SqlFunctions::class.java,
        "timeWithLocalTimeZoneToTimestampWithLocalTimeZone", String::class.java, Int::class.javaPrimitiveType
    ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_STRING(
        SqlFunctions::class.java, "timeWithLocalTimeZoneToString",
        Int::class.javaPrimitiveType, TimeZone::class.java
    ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE(
        SqlFunctions::class.java, "timestampWithLocalTimeZoneToDate",
        Long::class.javaPrimitiveType, TimeZone::class.java
    ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME(
        SqlFunctions::class.java, "timestampWithLocalTimeZoneToTime",
        Long::class.javaPrimitiveType, TimeZone::class.java
    ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE(
        SqlFunctions::class.java,
        "timestampWithLocalTimeZoneToTimeWithLocalTimeZone", Long::class.javaPrimitiveType
    ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(
        SqlFunctions::class.java,
        "timestampWithLocalTimeZoneToTimestamp", Long::class.javaPrimitiveType, TimeZone::class.java
    ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING(
        SqlFunctions::class.java,
        "timestampWithLocalTimeZoneToString", Long::class.javaPrimitiveType, TimeZone::class.java
    ),
    UNIX_DATE_TO_STRING(
        DateTimeUtils::class.java, "unixDateToString", Int::class.javaPrimitiveType
    ),
    UNIX_TIME_TO_STRING(
        DateTimeUtils::class.java,
        "unixTimeToString",
        Int::class.javaPrimitiveType
    ),
    UNIX_TIMESTAMP_TO_STRING(
        DateTimeUtils::class.java, "unixTimestampToString",
        Long::class.javaPrimitiveType
    ),
    INTERVAL_YEAR_MONTH_TO_STRING(
        DateTimeUtils::class.java,
        "intervalYearMonthToString", Int::class.javaPrimitiveType, TimeUnitRange::class.java
    ),
    INTERVAL_DAY_TIME_TO_STRING(
        DateTimeUtils::class.java, "intervalDayTimeToString",
        Long::class.javaPrimitiveType, TimeUnitRange::class.java, Int::class.javaPrimitiveType
    ),
    UNIX_DATE_EXTRACT(
        DateTimeUtils::class.java, "unixDateExtract",
        TimeUnitRange::class.java, Long::class.javaPrimitiveType
    ),
    UNIX_DATE_FLOOR(
        DateTimeUtils::class.java, "unixDateFloor",
        TimeUnitRange::class.java, Int::class.javaPrimitiveType
    ),
    UNIX_DATE_CEIL(
        DateTimeUtils::class.java, "unixDateCeil",
        TimeUnitRange::class.java, Int::class.javaPrimitiveType
    ),
    UNIX_TIMESTAMP_FLOOR(
        DateTimeUtils::class.java, "unixTimestampFloor",
        TimeUnitRange::class.java, Long::class.javaPrimitiveType
    ),
    UNIX_TIMESTAMP_CEIL(
        DateTimeUtils::class.java, "unixTimestampCeil",
        TimeUnitRange::class.java, Long::class.javaPrimitiveType
    ),
    LAST_DAY(SqlFunctions::class.java, "lastDay", Int::class.javaPrimitiveType), DAYNAME_WITH_TIMESTAMP(
        SqlFunctions::class.java,
        "dayNameWithTimestamp", Long::class.javaPrimitiveType, Locale::class.java
    ),
    DAYNAME_WITH_DATE(
        SqlFunctions::class.java,
        "dayNameWithDate", Int::class.javaPrimitiveType, Locale::class.java
    ),
    MONTHNAME_WITH_TIMESTAMP(
        SqlFunctions::class.java,
        "monthNameWithTimestamp", Long::class.javaPrimitiveType, Locale::class.java
    ),
    MONTHNAME_WITH_DATE(
        SqlFunctions::class.java,
        "monthNameWithDate", Int::class.javaPrimitiveType, Locale::class.java
    ),
    CURRENT_TIMESTAMP(SqlFunctions::class.java, "currentTimestamp", DataContext::class.java), CURRENT_TIME(
        SqlFunctions::class.java, "currentTime", DataContext::class.java
    ),
    CURRENT_DATE(SqlFunctions::class.java, "currentDate", DataContext::class.java), LOCAL_TIMESTAMP(
        SqlFunctions::class.java, "localTimestamp", DataContext::class.java
    ),
    LOCAL_TIME(SqlFunctions::class.java, "localTime", DataContext::class.java), TIME_ZONE(
        SqlFunctions::class.java, "timeZone", DataContext::class.java
    ),
    USER(SqlFunctions::class.java, "user", DataContext::class.java), SYSTEM_USER(
        SqlFunctions::class.java, "systemUser", DataContext::class.java
    ),
    LOCALE(SqlFunctions::class.java, "locale", DataContext::class.java), BOOLEAN_TO_STRING(
        SqlFunctions::class.java, "toString", Boolean::class.javaPrimitiveType
    ),
    JDBC_ARRAY_TO_LIST(SqlFunctions::class.java, "arrayToList", java.sql.Array::class.java), OBJECT_TO_STRING(
        Object::class.java, "toString"
    ),
    OBJECTS_EQUAL(Objects::class.java, "equals", Object::class.java, Object::class.java), HASH(
        Utilities::class.java, "hash", Int::class.javaPrimitiveType, Object::class.java
    ),
    COMPARE(Utilities::class.java, "compare", Comparable::class.java, Comparable::class.java), COMPARE_NULLS_FIRST(
        Utilities::class.java, "compareNullsFirst", Comparable::class.java,
        Comparable::class.java
    ),
    COMPARE_NULLS_LAST(
        Utilities::class.java, "compareNullsLast", Comparable::class.java,
        Comparable::class.java
    ),
    COMPARE2(
        Utilities::class.java,
        "compare",
        Comparable::class.java,
        Comparable::class.java,
        Comparator::class.java
    ),
    COMPARE_NULLS_FIRST2(
        Utilities::class.java, "compareNullsFirst", Comparable::class.java,
        Comparable::class.java, Comparator::class.java
    ),
    COMPARE_NULLS_LAST2(
        Utilities::class.java, "compareNullsLast", Comparable::class.java,
        Comparable::class.java, Comparator::class.java
    ),
    ROUND_LONG(
        SqlFunctions::class.java,
        "round",
        Long::class.javaPrimitiveType,
        Long::class.javaPrimitiveType
    ),
    ROUND_INT(
        SqlFunctions::class.java, "round", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType
    ),
    DATE_TO_INT(
        SqlFunctions::class.java, "toInt", java.util.Date::class.java
    ),
    DATE_TO_INT_OPTIONAL(
        SqlFunctions::class.java, "toIntOptional",
        java.util.Date::class.java
    ),
    TIME_TO_INT(SqlFunctions::class.java, "toInt", Time::class.java), TIME_TO_INT_OPTIONAL(
        SqlFunctions::class.java, "toIntOptional", Time::class.java
    ),
    TIMESTAMP_TO_LONG(SqlFunctions::class.java, "toLong", java.util.Date::class.java), TIMESTAMP_TO_LONG_OFFSET(
        SqlFunctions::class.java, "toLong", java.util.Date::class.java,
        TimeZone::class.java
    ),
    TIMESTAMP_TO_LONG_OPTIONAL(
        SqlFunctions::class.java, "toLongOptional",
        Timestamp::class.java
    ),
    TIMESTAMP_TO_LONG_OPTIONAL_OFFSET(
        SqlFunctions::class.java, "toLongOptional",
        Timestamp::class.java, TimeZone::class.java
    ),
    SEQUENCE_CURRENT_VALUE(
        SqlFunctions::class.java, "sequenceCurrentValue",
        String::class.java
    ),
    SEQUENCE_NEXT_VALUE(SqlFunctions::class.java, "sequenceNextValue", String::class.java), SLICE(
        SqlFunctions::class.java, "slice", List::class.java
    ),
    ELEMENT(SqlFunctions::class.java, "element", List::class.java), MEMBER_OF(
        SqlFunctions::class.java, "memberOf", Object::class.java, Collection::class.java
    ),
    MULTISET_INTERSECT_DISTINCT(
        SqlFunctions::class.java, "multisetIntersectDistinct",
        Collection::class.java, Collection::class.java
    ),
    MULTISET_INTERSECT_ALL(
        SqlFunctions::class.java, "multisetIntersectAll",
        Collection::class.java, Collection::class.java
    ),
    MULTISET_EXCEPT_DISTINCT(
        SqlFunctions::class.java, "multisetExceptDistinct",
        Collection::class.java, Collection::class.java
    ),
    MULTISET_EXCEPT_ALL(
        SqlFunctions::class.java, "multisetExceptAll",
        Collection::class.java, Collection::class.java
    ),
    MULTISET_UNION_DISTINCT(
        SqlFunctions::class.java, "multisetUnionDistinct",
        Collection::class.java, Collection::class.java
    ),
    MULTISET_UNION_ALL(
        SqlFunctions::class.java, "multisetUnionAll", Collection::class.java,
        Collection::class.java
    ),
    IS_A_SET(SqlFunctions::class.java, "isASet", Collection::class.java), IS_EMPTY(
        Collection::class.java,
        "isEmpty"
    ),
    SUBMULTISET_OF(
        SqlFunctions::class.java, "submultisetOf", Collection::class.java,
        Collection::class.java
    ),
    ARRAY_REVERSE(SqlFunctions::class.java, "reverse", List::class.java), SELECTIVITY(
        Selectivity::class.java, "getSelectivity", RexNode::class.java
    ),
    UNIQUE_KEYS(UniqueKeys::class.java, "getUniqueKeys", Boolean::class.javaPrimitiveType), AVERAGE_ROW_SIZE(
        Size::class.java, "averageRowSize"
    ),
    AVERAGE_COLUMN_SIZES(Size::class.java, "averageColumnSizes"), IS_PHASE_TRANSITION(
        Parallelism::class.java, "isPhaseTransition"
    ),
    SPLIT_COUNT(Parallelism::class.java, "splitCount"), LOWER_BOUND_COST(
        LowerBoundCost::class.java, "getLowerBoundCost",
        VolcanoPlanner::class.java
    ),
    MEMORY(Memory::class.java, "memory"), CUMULATIVE_MEMORY_WITHIN_PHASE(
        Memory::class.java,
        "cumulativeMemoryWithinPhase"
    ),
    CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT(
        Memory::class.java,
        "cumulativeMemoryWithinPhaseSplit"
    ),
    COLUMN_UNIQUENESS(
        ColumnUniqueness::class.java, "areColumnsUnique",
        ImmutableBitSet::class.java, Boolean::class.javaPrimitiveType
    ),
    COLLATIONS(Collation::class.java, "collations"), DISTRIBUTION(
        Distribution::class.java, "distribution"
    ),
    NODE_TYPES(NodeTypes::class.java, "getNodeTypes"), ROW_COUNT(
        RowCount::class.java, "getRowCount"
    ),
    MAX_ROW_COUNT(MaxRowCount::class.java, "getMaxRowCount"), MIN_ROW_COUNT(
        MinRowCount::class.java, "getMinRowCount"
    ),
    DISTINCT_ROW_COUNT(
        DistinctRowCount::class.java, "getDistinctRowCount",
        ImmutableBitSet::class.java, RexNode::class.java
    ),
    PERCENTAGE_ORIGINAL_ROWS(
        PercentageOriginalRows::class.java,
        "getPercentageOriginalRows"
    ),
    POPULATION_SIZE(
        PopulationSize::class.java, "getPopulationSize",
        ImmutableBitSet::class.java
    ),
    COLUMN_ORIGIN(ColumnOrigin::class.java, "getColumnOrigins", Int::class.javaPrimitiveType), EXPRESSION_LINEAGE(
        ExpressionLineage::class.java, "getExpressionLineage", RexNode::class.java
    ),
    TABLE_REFERENCES(TableReferences::class.java, "getTableReferences"), CUMULATIVE_COST(
        CumulativeCost::class.java, "getCumulativeCost"
    ),
    NON_CUMULATIVE_COST(NonCumulativeCost::class.java, "getNonCumulativeCost"), PREDICATES(
        Predicates::class.java, "getPredicates"
    ),
    ALL_PREDICATES(AllPredicates::class.java, "getAllPredicates"), EXPLAIN_VISIBILITY(
        ExplainVisibility::class.java, "isVisibleInExplain",
        SqlExplainLevel::class.java
    ),
    SCALAR_EXECUTE1(Scalar::class.java, "execute", Context::class.java), SCALAR_EXECUTE2(
        Scalar::class.java, "execute", Context::class.java, Array<Object>::class.java
    ),
    CONTEXT_VALUES(Context::class.java, "values", true), CONTEXT_ROOT(
        Context::class.java, "root", true
    ),
    FUNCTION_CONTEXTS_OF(
        FunctionContexts::class.java, "of", DataContext::class.java,
        Array<Object>::class.java
    ),
    DATA_CONTEXT_GET_QUERY_PROVIDER(DataContext::class.java, "getQueryProvider"), METADATA_REL(
        Metadata::class.java, "rel"
    ),
    STRUCT_ACCESS(
        SqlFunctions::class.java, "structAccess", Object::class.java, Int::class.javaPrimitiveType,
        String::class.java
    ),
    SOURCE_SORTER(
        SourceSorter::class.java, Function2::class.java, Function1::class.java,
        Comparator::class.java
    ),
    BASIC_LAZY_ACCUMULATOR(BasicLazyAccumulator::class.java, Function2::class.java), LAZY_AGGREGATE_LAMBDA_FACTORY(
        LazyAggregateLambdaFactory::class.java,
        Function0::class.java, List::class.java
    ),
    BASIC_AGGREGATE_LAMBDA_FACTORY(
        BasicAggregateLambdaFactory::class.java,
        Function0::class.java, List::class.java
    ),
    AGG_LAMBDA_FACTORY_ACC_INITIALIZER(
        AggregateLambdaFactory::class.java,
        "accumulatorInitializer"
    ),
    AGG_LAMBDA_FACTORY_ACC_ADDER(
        AggregateLambdaFactory::class.java,
        "accumulatorAdder"
    ),
    AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR(
        AggregateLambdaFactory::class.java,
        "resultSelector", Function2::class.java
    ),
    AGG_LAMBDA_FACTORY_ACC_SINGLE_GROUP_RESULT_SELECTOR(
        AggregateLambdaFactory::class.java,
        "singleGroupResultSelector", Function1::class.java
    ),
    TUMBLING(EnumUtils::class.java, "tumbling", Enumerable::class.java, Function1::class.java), HOPPING(
        EnumUtils::class.java,
        "hopping",
        Enumerator::class.java,
        Int::class.javaPrimitiveType,
        Long::class.javaPrimitiveType,
        Long::class.javaPrimitiveType,
        Long::class.javaPrimitiveType
    ),
    SESSIONIZATION(
        EnumUtils::class.java,
        "sessionize",
        Enumerator::class.java,
        Int::class.javaPrimitiveType,
        Int::class.javaPrimitiveType,
        Long::class.javaPrimitiveType
    ),
    BIG_DECIMAL_ADD(BigDecimal::class.java, "add", BigDecimal::class.java), BIG_DECIMAL_NEGATE(
        BigDecimal::class.java, "negate"
    ),
    COMPARE_TO(Comparable::class.java, "compareTo", Object::class.java);

    @SuppressWarnings("ImmutableEnumChecker")
    val method: Method

    @SuppressWarnings("ImmutableEnumChecker")
    val constructor: Constructor

    @SuppressWarnings("ImmutableEnumChecker")
    val field: Field

    init {
        // TODO: split enum in three different ones
        this.method = castNonNull(method)
        this.constructor = castNonNull(constructor)
        this.field = castNonNull(field)
    }

    /** Defines a method.  */
    constructor(clazz: Class, methodName: String, vararg argumentTypes: Class) : this(
        Types.lookupMethod(
            clazz,
            methodName,
            argumentTypes
        ), null, null
    ) {
    }

    /** Defines a constructor.  */
    constructor(clazz: Class, vararg argumentTypes: Class) : this(
        null,
        Types.lookupConstructor(clazz, argumentTypes),
        null
    ) {
    }

    /** Defines a field.  */
    constructor(clazz: Class, fieldName: String, dummy: Boolean) : this(
        null,
        null,
        Types.lookupField(clazz, fieldName)
    ) {
        assert(dummy) { "dummy value for method overloading must be true" }
    }

    val methodName: String
        get() = castNonNull(method).getName()

    companion object {
        val MAP: ImmutableMap<Method, BuiltInMethod>? = null

        init {
            val builder: ImmutableMap.Builder<Method, BuiltInMethod> = ImmutableMap.builder()
            for (value in values()) {
                if (org.apache.calcite.util.value.method != null) {
                    org.apache.calcite.util.builder.put(
                        org.apache.calcite.util.value.method,
                        org.apache.calcite.util.value
                    )
                }
            }
            MAP = org.apache.calcite.util.builder.build()
        }
    }
}
