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

import org.apache.calcite.linq4j.function.Function1

/**
 * Helps to run benchmarks by running the same task repeatedly and averaging
 * the running times.
 */
class Benchmark(
    description: String, function: Function1<Statistician?, Void>,
    repeat: Int
) {
    private val function: Function1<Statistician, Void>
    private val repeat: Int
    private val statistician: Statistician

    init {
        this.function = function
        this.repeat = repeat
        statistician = Statistician(description)
    }

    fun run() {
        for (i in 0 until repeat) {
            function.apply(statistician)
        }
        statistician.printDurations()
    }

    /**
     * Collects statistics for a test that is run multiple times.
     */
    class Statistician(private val desc: String) {
        private val durations: List<Long> = ArrayList()
        fun record(start: Long) {
            durations.add(
                printDuration(
                    desc + " iteration #" + (durations.size() + 1), start
                )
            )
        }

        fun printDurations() {
            if (!LOGGER.isDebugEnabled()) {
                return
            }
            var coreDurations = durations
            val durationsString = durations.toString() // save before sort

            // Ignore the first 3 readings. (JIT compilation takes a while to
            // kick in.)
            if (coreDurations.size() > 3) {
                coreDurations = durations.subList(3, durations.size())
            }
            Collections.sort(coreDurations)
            // Further ignore the max and min.
            var coreCoreDurations = coreDurations
            if (coreDurations.size() > 4) {
                coreCoreDurations = coreDurations.subList(1, coreDurations.size() - 1)
            }
            var sum: Long = 0
            val count: Int = coreCoreDurations.size()
            for (duration in coreCoreDurations) {
                sum += duration
            }
            val avg = sum.toDouble() / count
            var y = 0.0
            for (duration in coreCoreDurations) {
                val x = duration - avg
                y += x * x
            }
            val stddev: Double = Math.sqrt(y / count)
            if (durations.size() === 0) {
                LOGGER.debug("{}: {}", desc, "no runs")
            } else {
                LOGGER.debug(
                    "{}: {} first; {} +- {}; {} min; {} max; {} nanos",
                    desc,
                    durations[0], avg, stddev, coreDurations[0],
                    Util.last(coreDurations), durationsString
                )
            }
        }
    }

    companion object {
        /**
         * Certain tests are enabled only if logging is enabled at debug level or
         * higher.
         */
        val LOGGER: Logger = LoggerFactory.getLogger(Benchmark::class.java)

        /**
         * Returns whether performance tests are enabled.
         */
        fun enabled(): Boolean {
            return LOGGER.isDebugEnabled()
        }

        fun printDuration(desc: String?, t0: Long): Long {
            val t1: Long = System.nanoTime()
            val duration = t1 - t0
            LOGGER.debug("{} took {} nanos", desc, duration)
            return duration
        }
    }
}
