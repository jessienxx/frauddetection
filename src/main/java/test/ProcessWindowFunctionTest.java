package test;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class ProcessWindowFunctionTest {


	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final boolean fileOutput = params.has("output");
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Long>> source = env.fromElements(
				Tuple2.of("a", 1L),
				Tuple2.of("b", 1L),
				Tuple2.of("b", 3L),
				Tuple2.of("b", 5L),
				Tuple2.of("c", 6L),
				Tuple2.of("a", 10L),
				Tuple2.of("c", 11L));

		DataStream<Tuple2<String, Double>> aggregated = source
				.keyBy(new Tuple2KeySelector())
				.timeWindow(Time.milliseconds(1))
				.aggregate(new AverageAggregate(), new MyProcessWindowFunction());


		if (fileOutput) {
			aggregated.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			aggregated
					.print();
		}

		env.execute("Streaming Iteration Example");
	}
		// ------------------------------------------------------------------------
	private static class AverageAggregate
			implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
		@Override
		public Tuple2<Long, Long> createAccumulator() {
			return new Tuple2<>(0L, 0L);
		}

		@Override
		public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
			return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
		}

		@Override
		public Double getResult(Tuple2<Long, Long> accumulator) {
			return ((double) accumulator.f0) / accumulator.f1;
		}

		@Override
		public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
			return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
		}
	}

	private static class MyProcessWindowFunction
			extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

		public void process(String key,
							Context context,
							Iterable<Double> averages,
							Collector<Tuple2<String, Double>> out) {
			Double average = averages.iterator().next();
			out.collect(new Tuple2<>(key, average));
		}
	}

	private static class Tuple2KeySelector implements KeySelector<Tuple2<String, Long>, String> {

		@Override
		public String getKey(Tuple2<String, Long> value) throws Exception {
			return value.f0;
		}
	}

}
