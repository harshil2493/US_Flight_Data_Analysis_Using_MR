import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SevenReducer extends
		Reducer<Text, SevenCustomizedDataType, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<SevenCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String readKey = key.toString();
		String data = readKey.substring(0, 1);
		
		if(data.contains("P"))
		{
			int count = 0;
			Float sum = (float) 0;
			for (SevenCustomizedDataType customizedDataType : values) {
				sum = sum + (float) customizedDataType.getPercent().get();
				count = count + customizedDataType.getCount().get();
			}

			context.write(NullWritable.get(), new Text(key.toString() + "&"
					+ (sum * 1.0f / count)));
		}
//		else if(data.contains("B"))
//		{
//			for()
//		}
		else
		{
		int count = 0;
		Float sum = (float) 0;
		for (SevenCustomizedDataType customizedDataType : values) {
			sum = sum + (float) customizedDataType.getTotalDelay().get();
			count = count + customizedDataType.getCount().get();
		}

		context.write(NullWritable.get(), new Text(key.toString() + "&"
				+ (sum * 1.0f / count)));
		}
	}

}
