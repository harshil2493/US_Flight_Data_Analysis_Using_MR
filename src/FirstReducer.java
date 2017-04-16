import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends
		Reducer<Text, FirstCustomizedDataType, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<FirstCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String readKey = key.toString();
		String data = readKey.substring(0, 1);
		int count = 0;
		Float sum = (float) 0;
		for (FirstCustomizedDataType customizedDataType : values) {
			sum = sum + (float) customizedDataType.getTotalDelay().get();
			count = count + customizedDataType.getCount().get();
		}

		context.write(NullWritable.get(), new Text(key.toString() + "&"
				+ (sum * 1.0f / count)));

	}

}
