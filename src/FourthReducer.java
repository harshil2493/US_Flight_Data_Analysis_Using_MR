import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FourthReducer extends
		Reducer<Text, FourthCustomizedDataType, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<FourthCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String keyRead = key.toString();
		int totalCounts = 0;
		int totalDelay = 0;

		String cityName = "";

		for (FourthCustomizedDataType allValues : values) {
			boolean flag = allValues.getFlag().get();
			String dataRead = allValues.getDataLine().toString();
			if (flag) {
				if (cityName.equals("")) {
					cityName = dataRead;
				}
			} else {
				totalCounts += 1;
				totalDelay += Integer.parseInt(dataRead);

			}
		}

		context.write(NullWritable.get(), new Text(keyRead + "==" + cityName
				+ "#" + totalCounts + "#" + totalDelay));
	}

}
