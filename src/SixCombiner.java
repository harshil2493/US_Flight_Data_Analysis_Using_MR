import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SixCombiner extends
		Reducer<Text, SixCustomizedDataType, Text, SixCustomizedDataType> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<SixCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		// String keyValue = key.toString();
		// Map<Integer, Integer> combineYearToCount = new HashMap<Integer,
		// Integer>();
		// for (ThirdCustomizedDataType allValues : values) {
		// IntWritable flag = allValues.getCount();
		// String dataline = allValues.getDataLine().toString();
		// if (flag.get() == 0) {
		// context.write(key, allValues);
		// } else {
		//
		//
		// int year = Integer.parseInt(dataline);
		// if(combineYearToCount.containsKey(year))
		// {
		// combineYearToCount.put(year, 1 + combineYearToCount.get(year));
		// }
		// else
		// {
		// combineYearToCount.put(year, 1);
		// }
		// }
		// }
		// for (Integer years : combineYearToCount.keySet()) {
		// ThirdCustomizedDataType thirdCustomizedDataTypeInCombiner = new
		// ThirdCustomizedDataType();
		// thirdCustomizedDataTypeInCombiner.setCount(new
		// IntWritable(combineYearToCount.get(years)));
		// thirdCustomizedDataTypeInCombiner.setDataLine(new Text(years+""));
		//
		// context.write(key, thirdCustomizedDataTypeInCombiner);
		// }
		Map<Integer, Integer> yearlyDelayedCount = new HashMap<Integer, Integer>();
		Map<Integer, Integer> yearlyDelayedMinutes = new HashMap<Integer, Integer>();
		// int totalCount = 0;
		// int totalDelay = 0;

		for (SixCustomizedDataType allValues : values) {
			if (allValues.getFlag().get()) {
				context.write(key, allValues);
			} else {
				int year = allValues.getDataLine().get();
				if (yearlyDelayedCount.containsKey(year)) {
					yearlyDelayedCount.put(year, yearlyDelayedCount.get(year)
							+ allValues.getTotalCount().get());
					yearlyDelayedMinutes.put(year,
							yearlyDelayedMinutes.get(year)
									+ allValues.getTotalDelay().get());

				} else {
					yearlyDelayedCount.put(year, allValues.getTotalCount()
							.get());
					yearlyDelayedMinutes.put(year, allValues.getTotalDelay()
							.get());
				}
				// totalCount = totalCount + allValues.getTotalCount().get();
				// totalDelay = totalDelay + allValues.getTotalDelay().get();
			}
		}
		for (Integer currentYear : yearlyDelayedCount.keySet()) {
			SixCustomizedDataType sixCustomizedDataType = new SixCustomizedDataType();
			sixCustomizedDataType.setFlag(new BooleanWritable(false));
			sixCustomizedDataType.setTotalDelay(new IntWritable(
					yearlyDelayedMinutes.get(currentYear)));
			sixCustomizedDataType.setTotalCount(new IntWritable(
					yearlyDelayedCount.get(currentYear)));
			sixCustomizedDataType.setDataLine(new IntWritable(currentYear));

			context.write(key, sixCustomizedDataType);
		}

	}

}
