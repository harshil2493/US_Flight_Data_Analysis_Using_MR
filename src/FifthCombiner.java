import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FifthCombiner extends
		Reducer<Text, FifthCustomizedDataType, Text, FifthCustomizedDataType> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<FifthCustomizedDataType> values,
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

		int totalCount = 0;
		int totalDelay = 0;

		for (FifthCustomizedDataType allValues : values) {
			if (allValues.getFlag().get()) {
				context.write(key, allValues);
			} else {
				totalCount = totalCount + allValues.getTotalCount().get();
				totalDelay = totalDelay + allValues.getTotalDelay().get();
			}
		}

		FifthCustomizedDataType fifthCustomizedDataType = new FifthCustomizedDataType();
		fifthCustomizedDataType.setFlag(new BooleanWritable(false));
		fifthCustomizedDataType.setTotalDelay(new IntWritable(totalDelay));
		fifthCustomizedDataType.setTotalCount(new IntWritable(totalCount));
		fifthCustomizedDataType.setDataLine(new Text(""));
		context.write(key, fifthCustomizedDataType);

	}

}
