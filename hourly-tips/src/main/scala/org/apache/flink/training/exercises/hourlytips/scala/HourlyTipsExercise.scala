/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips.scala

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase._
import org.apache.flink.util.Collector

/**
  * The "Hourly Tips" exercise of the Flink training in the docs.
  *
  * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
  * then from that stream, find the highest tip total in each hour.
  *
  */
object HourlyTipsExercise {

  def main(args: Array[String]) {

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    // start the data generator
    val fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()))

    val hourlySum = fares.keyBy(_.driverId)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .process(new WindowSum)

    val hourlyMax = hourlySum
      .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
      .maxBy(2)

   // print result on stdout
    printOrTest(hourlyMax)

    // execute the transformation pipeline
    env.execute("Hourly Tips (scala)")
  }

}

class WindowSum extends ProcessWindowFunction[TaxiFare, (Long, Long, Float), Long, TimeWindow] {
  override def process(driverId: Long, context: Context, fares: Iterable[TaxiFare], out: Collector[(Long, Long, Float)]): Unit = {
    out.collect((context.window.getEnd, driverId, fares.map(_.tip).sum))
  }
}
