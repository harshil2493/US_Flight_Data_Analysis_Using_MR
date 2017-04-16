import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdCombiner extends
		Reducer<Text, ThirdCustomizedDataType, Text, ThirdCustomizedDataType> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<ThirdCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String keyValue = key.toString();
		Map<Integer, Integer> combineYearToCount = new HashMap<Integer, Integer>();
		for (ThirdCustomizedDataType allValues : values) {
			IntWritable flag = allValues.getCount();
			String dataline = allValues.getDataLine().toString();
			if (flag.get() == 0) {
				context.write(key, allValues);
			} else {

				int year = Integer.parseInt(dataline);
				if (combineYearToCount.containsKey(year)) {
					combineYearToCount.put(year,
							1 + combineYearToCount.get(year));
				} else {
					combineYearToCount.put(year, 1);
				}
			}
		}
		for (Integer years : combineYearToCount.keySet()) {
			ThirdCustomizedDataType thirdCustomizedDataTypeInCombiner = new ThirdCustomizedDataType();
			thirdCustomizedDataTypeInCombiner.setCount(new IntWritable(
					combineYearToCount.get(years)));
			thirdCustomizedDataTypeInCombiner.setDataLine(new Text(years + ""));

			context.write(key, thirdCustomizedDataTypeInCombiner);
		}

	}

}
