import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends
		Reducer<Text, ThirdCustomizedDataType, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	@Override
	protected void reduce(Text key, Iterable<ThirdCustomizedDataType> values,
			Context context) throws IOException, InterruptedException {

		String keyRead = key.toString();
		int totalSum = 0;
		String answerString = "";
		String airportName = "";
		Map<Integer, Integer> totalCountToYear = new HashMap<Integer, Integer>();
		for (ThirdCustomizedDataType allValues : values) {
			int count = allValues.getCount().get();
			String dataRead = allValues.getDataLine().toString();
			if (count == 0) {
				if (airportName.equals("")) {
					airportName = dataRead;
				}
			} else {
				totalSum += count;

				int year = Integer.parseInt(dataRead);
				if (totalCountToYear.containsKey(year)) {
					totalCountToYear.put(year,
							count + totalCountToYear.get(year));
				} else {
					totalCountToYear.put(year, count);
				}

			}
		}
		for (Integer years : totalCountToYear.keySet()) {

			answerString += years + "&&&" + totalCountToYear.get(years) + "@";
		}
		if (answerString != null && !answerString.isEmpty()) {
			answerString = answerString.substring(0, answerString.length() - 1);
			context.write(NullWritable.get(), new Text(keyRead + "=="
					+ airportName + "#" + totalSum + "#" + answerString));
		}

	}

}
