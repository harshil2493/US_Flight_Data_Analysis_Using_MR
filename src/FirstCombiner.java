import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstCombiner extends
		Reducer<Text, FirstCustomizedDataType, Text, FirstCustomizedDataType> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<FirstCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String readKey = key.toString();
		String data = readKey.substring(0, 1);
		int count = 0;
		int sum = 0;
		for (FirstCustomizedDataType customizedDataType : values) {
			sum = sum + customizedDataType.getTotalDelay().get();
			count = count + customizedDataType.getCount().get();
		}
		FirstCustomizedDataType firstCustomizedDataType = new FirstCustomizedDataType();
		firstCustomizedDataType.setCount(new IntWritable(count));
		firstCustomizedDataType.setTotalDelay(new IntWritable(sum));
		context.write(key, firstCustomizedDataType);

	}

}
