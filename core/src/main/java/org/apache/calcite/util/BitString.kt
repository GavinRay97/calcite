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

import java.math.BigInteger

/**
 * String of bits.
 *
 *
 * A bit string logically consists of a set of '0' and '1' values, of a
 * specified length. The length is preserved even if this means that the bit
 * string has leading '0's.
 *
 *
 * You can create a bit string from a string of 0s and 1s
 * ([.BitString] or [.createFromBitString]), or from a
 * string of hex digits ([.createFromHexString]). You can convert it to a
 * byte array ([.getAsByteArray]), to a bit string ([.toBitString]),
 * or to a hex string ([.toHexString]). A utility method
 * [.toByteArrayFromBitString] converts a bit string directly to a byte
 * array.
 *
 *
 * This class is immutable: once created, none of the methods modify the
 * value.
 */
class BitString protected constructor(
    bits: String,
    bitCount: Int
) {
    //~ Instance fields --------------------------------------------------------
    private val bits: String
    val bitCount: Int

    //~ Constructors -----------------------------------------------------------
    init {
        assert(
            bits.replace("1", "").replace("0", "").length() === 0
        ) { "bit string '$bits' contains digits other than {0, 1}" }
        this.bits = bits
        this.bitCount = bitCount
    }

    @Override
    override fun toString(): String {
        return toBitString()
    }

    @Override
    override fun hashCode(): Int {
        return bits.hashCode() + bitCount
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return (o === this
                || (o is BitString
                && bits.equals((o as BitString).bits)
                && bitCount == (o as BitString).bitCount))
    }

    val asByteArray: ByteArray
        get() = toByteArrayFromBitString(bits, bitCount)

    /**
     * Returns this bit string as a bit string, such as "10110".
     */
    fun toBitString(): String {
        return bits
    }

    /**
     * Converts this bit string to a hex string, such as "7AB".
     */
    fun toHexString(): String {
        val bytes = asByteArray
        val s: String = ConversionUtil.toStringFromByteArray(bytes, 16)
        when (bitCount % 8) {
            1, 2, 3, 4 -> return s.substring(1)
            5, 6, 7, 0 -> return s
            else -> {}
        }
        return if (bitCount % 8 == 4) {
            s.substring(1)
        } else {
            s
        }
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        /**
         * Creates a BitString representation out of a Hex String. Initial zeros are
         * be preserved. Hex String is defined in the SQL standard to be a string
         * with odd number of hex digits. An even number of hex digits is in the
         * standard a Binary String.
         *
         * @param s a string, in hex notation
         * @throws NumberFormatException if `s` is invalid.
         */
        fun createFromHexString(s: String): BitString {
            val bitCount: Int = s.length() * 4
            val bits = if (bitCount == 0) "" else BigInteger(s, 16).toString(2)
            return BitString(bits, bitCount)
        }

        /**
         * Creates a BitString representation out of a Bit String. Initial zeros are
         * be preserved.
         *
         * @param s a string of 0s and 1s.
         * @throws NumberFormatException if `s` is invalid.
         */
        fun createFromBitString(s: String): BitString {
            val n: Int = s.length()
            if (n > 0) { // check that S is valid
                Util.discard(BigInteger(s, 2))
            }
            return BitString(s, n)
        }

        /**
         * Converts a bit string to an array of bytes.
         */
        fun toByteArrayFromBitString(
            bits: String,
            bitCount: Int
        ): ByteArray {
            if (bitCount < 0) {
                return ByteArray(0)
            }
            val byteCount = (bitCount + 7) / 8
            val srcBytes: ByteArray
            srcBytes = if (bits.length() > 0) {
                val bigInt = BigInteger(bits, 2)
                bigInt.toByteArray()
            } else {
                ByteArray(0)
            }
            val dest = ByteArray(byteCount)

            // If the number started with 0s, the array won't be very long. Assume
            // that ret is already initialized to 0s, and just copy into the
            // RHS of it.
            val bytesToCopy: Int = Math.min(byteCount, srcBytes.size)
            System.arraycopy(
                srcBytes,
                srcBytes.size - bytesToCopy,
                dest,
                dest.size - bytesToCopy,
                bytesToCopy
            )
            return dest
        }

        /**
         * Concatenates some BitStrings. Concatenates all at once, not pairwise, to
         * avoid string copies.
         *
         * @param args BitString[]
         */
        fun concat(args: List<BitString>): BitString {
            if (args.size() < 2) {
                return args[0]
            }
            var length = 0
            for (arg in args) {
                length += arg.bitCount
            }
            val sb = StringBuilder(length)
            for (arg1 in args) {
                sb.append(arg1.bits)
            }
            return BitString(
                sb.toString(),
                length
            )
        }

        /**
         * Creates a BitString from an array of bytes.
         *
         * @param bytes Bytes
         * @return BitString
         */
        fun createFromBytes(bytes: ByteArray): BitString {
            val bitCount: Int = Objects.requireNonNull(bytes, "bytes").length * 8
            val sb = StringBuilder(bitCount)
            for (b in bytes) {
                val s: String = Integer.toBinaryString(Byte.toUnsignedInt(b))
                for (i in s.length()..7) {
                    sb.append('0') // pad to length 8
                }
                sb.append(s)
            }
            return BitString(sb.toString(), bitCount)
        }
    }
}
