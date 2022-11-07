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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

		DataStream<Tuple3<Long, Long, Float>> hourlySum = fares
				.keyBy(fare -> fare.driverId)
//				.window(TumblingEventTimeWindows.of(Time.hours(1)))
//				.process(new WindowSum());
				.process(new PseudoWindow(Time.hours(1)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlySum
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
				.maxBy(2);

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

}

class WindowSum extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
	@Override
	public void process(
			Long driverId,
			Context context,
			Iterable<TaxiFare> fares,
			Collector<Tuple3<Long, Long, Float>> out) {
		float totalTip = StreamSupport.stream(fares.spliterator(), false)
				.map(fare -> fare.tip)
				.reduce(0f, Float::sum);
		out.collect(Tuple3.of(context.window().getEnd(), driverId, totalTip));
	}
}

class PseudoWindow extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {
	private final long durationMillis;
	private transient MapState<Long, Float> sumOfTips;

	public PseudoWindow(Time duration) {
		this.durationMillis = duration.toMilliseconds();
	}

	@Override
	public void open(Configuration conf) {
		sumOfTips = getRuntimeContext().getMapState(new MapStateDescriptor<>("sumOfTips", Long.class, Float.class));
	}

	@Override
	public void processElement(
			TaxiFare fare,
			Context ctx,
			Collector<Tuple3<Long, Long, Float>> out) throws Exception {
		long eventTime = fare.getEventTime();
		TimerService timerService = ctx.timerService();
		if (eventTime <= timerService.currentWatermark()) {
			// This event is late; its window has already been triggered.
			return;
		}
		// Round up eventTime to the end of the window containing this event.
		long endOfWindow = eventTime - (eventTime % durationMillis) + durationMillis;

		// Schedule a callback for when the window has been completed.
		timerService.registerEventTimeTimer(endOfWindow);

		// Add this fare's tip to the running total for that window.
		Float sum = sumOfTips.get(endOfWindow);
		if (sum == null) {
			sum = 0.0f;
		}
		sum += fare.tip;
		sumOfTips.put(endOfWindow, sum);
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<Tuple3<Long, Long, Float>> out) throws Exception {
		long driverId = ctx.getCurrentKey();

		// Look up the result for the hour that just ended.
		Float sumOfTips = this.sumOfTips.get(timestamp);

		Tuple3<Long, Long, Float> result = Tuple3.of(timestamp, driverId, sumOfTips);
		out.collect(result);
		this.sumOfTips.remove(timestamp);
	}
}
