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

package org.apache.flink.training.exercises.longrides.scala

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase._
import org.apache.flink.util.Collector

import scala.concurrent.duration.DurationInt

/**
  * The "Long Ride Alerts" exercise of the Flink training in the docs.
  *
  * The goal for this exercise is to emit START events for taxi rides that have not been matched
  * by an END event during the first 2 hours of the ride.
  *
  */
object LongRidesExercise {

  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))

    val longRides = rides
      .keyBy(_.rideId)
      .process(new ImplementMeFunction())

    printOrTest(longRides)

    env.execute("Long Taxi Rides")
  }

  class ImplementMeFunction extends KeyedProcessFunction[Long, TaxiRide, TaxiRide] {
    @transient
    private lazy val arrivedEvent = getRuntimeContext.getState(new ValueStateDescriptor[TaxiRide]("arrivedEvent", classOf[TaxiRide]))

    override def processElement(ride: TaxiRide,
                                context: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
                                out: Collector[TaxiRide]): Unit = {
      val timerService = context.timerService()
      val arrived = arrivedEvent.value()
      val twoHours = 2.hours.toMillis
      if (ride.isStart) {
        if (arrived == null) {
          // END event hasn't arrived yet
          arrivedEvent.update(ride)
          timerService.registerEventTimeTimer(ride.getEventTime + twoHours)
        } else {
          arrivedEvent.clear()
        }
      } else {
        if (arrived == null) {
          // START event hasn't arrived yet
          arrivedEvent.update(ride)
        } else {
          timerService.deleteEventTimeTimer(arrived.getEventTime + twoHours)
          arrivedEvent.clear()
        }
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#OnTimerContext,
                         out: Collector[TaxiRide]): Unit = {
      out.collect(arrivedEvent.value())
      arrivedEvent.clear()
    }

  }

}
