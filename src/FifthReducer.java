import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FifthReducer extends
		Reducer<Text, FifthCustomizedDataType, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<FifthCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		int totalCount = 0;
		int totalDelay = 0;

		String keyToWrite = "";

		for (FifthCustomizedDataType allValues : values) {
			if (allValues.getFlag().get()) {
				keyToWrite = key.toString() + "=="
						+ allValues.getDataLine().toString();
				// context.write(key, allValues);
			} else {
				totalCount = totalCount + allValues.getTotalCount().get();
				totalDelay = totalDelay + allValues.getTotalDelay().get();
			}
		}
		context.write(nullWrite, new Text(keyToWrite + "&&&" + totalCount
				+ "&&&" + totalDelay));

	}

}
