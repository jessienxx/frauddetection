package test;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class FlinkTest {


	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

			DataStream<Tuple3<String, String, Integer>> source = env.fromElements(
					Tuple3.of("hello", "hallo", 1),
					Tuple3.of("hello", "hallo", 2));

			DataStream<String> window = source
					.keyBy(new Tuple3KeySelector())
					.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
					.aggregate(new DummyAggregationFunction(), new TestProcessWindowFunction());

		window.print();
		env.execute("Streaming Iteration Example");
		// ------------------------------------------------------------------------
	}
	private static class Tuple3KeySelector implements KeySelector<Tuple3<String, String, Integer>, String> {

		@Override
		public String getKey(Tuple3<String, String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	private static class DummyAggregationFunction
			implements AggregateFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, Integer> {

		@Override
		public Tuple2<String, Integer> createAccumulator() {
			return new Tuple2<>("", 0);
		}

		@Override
		public Tuple2<String, Integer> add(Tuple3<String, String, Integer> value, Tuple2<String, Integer> accumulator) {
			accumulator.f0 = value.f0;
			accumulator.f1 = value.f2;
			return accumulator;
		}

		@Override
		public Integer getResult(Tuple2<String, Integer> accumulator) {
			return accumulator.f1;
		}

		@Override
		public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
			return a;
		}
	}
	private static class TestProcessWindowFunction
			extends ProcessWindowFunction<Integer, String, String, TimeWindow> {

		@Override
		public void process(String key,
							Context ctx,
							Iterable<Integer> values,
							Collector<String> out) throws Exception {

			for (Integer in : values) {
				out.collect(in.toString());
			}
		}
	}


}
