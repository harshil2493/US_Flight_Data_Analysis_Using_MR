import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SevenCombiner extends
		Reducer<Text, SevenCustomizedDataType, Text, SevenCustomizedDataType> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<SevenCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String readKey = key.toString();
		String data = readKey.substring(0, 1);
		if(data.contains("P"))
		{
			int count = 0;
			float sum = 0;
			for (SevenCustomizedDataType customizedDataType : values) {
				sum = sum + customizedDataType.getPercent().get();
				count = count + customizedDataType.getCount().get();
			}
			SevenCustomizedDataType sevenCustomizedDataType = new SevenCustomizedDataType();
			sevenCustomizedDataType.setCount(new IntWritable(count));
			sevenCustomizedDataType.setPercent(new FloatWritable(sum));
			context.write(key, sevenCustomizedDataType);
		}
		else
		{
		int count = 0;
		int sum = 0;
		for (SevenCustomizedDataType customizedDataType : values) {
			sum = sum + customizedDataType.getTotalDelay().get();
			count = count + customizedDataType.getCount().get();
		}
		SevenCustomizedDataType sevenCustomizedDataType = new SevenCustomizedDataType();
		sevenCustomizedDataType.setCount(new IntWritable(count));
		sevenCustomizedDataType.setTotalDelay(new IntWritable(sum));
		context.write(key, sevenCustomizedDataType);
		}
	}

}

