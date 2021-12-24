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

import java.util.Comparator

/**
 * Comparator that compares all strings differently, but if two strings are
 * equal in case-insensitive match they are right next to each other.
 *
 *
 * Note: strings that differ only in upper-lower case are treated by this comparator
 * as distinct.
 *
 *
 * In a collection sorted on this comparator, we can find case-insensitive matches
 * for a given string using
 * [.floorKey]
 * and [.ceilingKey].
 */
internal class CaseInsensitiveComparator : Comparator {
    /**
     * Enables to create floor and ceiling keys for given string.
     */
    private class Key(val value: String, val compareResult: Int) {
        @Override
        override fun toString(): String {
            return value
        }
    }

    fun floorKey(key: String): Object {
        return Key(key, -1)
    }

    fun ceilingKey(key: String): Object {
        return Key(key, 1)
    }

    @Override
    fun compare(o1: Object, o2: Object): Int {
        val s1: String = o1.toString()
        val s2: String = o2.toString()
        val c: Int = s1.compareToIgnoreCase(s2)
        if (c != 0) {
            return c
        }
        if (o1 is Key) {
            return (o1 as Key).compareResult
        }
        return if (o2 is Key) {
            -(o2 as Key).compareResult
        } else s1.compareTo(s2)
    }

    companion object {
        val COMPARATOR = CaseInsensitiveComparator()
    }
}
