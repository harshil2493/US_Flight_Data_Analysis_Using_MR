import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends
		Mapper<LongWritable, Text, Text, FirstCustomizedDataType> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String lineRead = value.toString();
		if (!(lineRead.startsWith("Year"))) {
			String[] parsedValues = lineRead.split(",");

			String year = (parsedValues[0]);
			String month = (parsedValues[1]);
			String dayOfWeek = (parsedValues[3]);

			String scheduledDepartureTime = (parsedValues[5]);
			String arrivalDelay = (parsedValues[14]);
			String departureDelay = (parsedValues[15]);

			//

			// String timeDepart = "";

			FirstCustomizedDataType customizedDataType = new FirstCustomizedDataType();

			// Question 1 & 2

			if (!arrivalDelay.contains("NA") || !departureDelay.contains("NA")) {

				// Setting Arrival
				int totalDelay = 0;
				if (!arrivalDelay.contains("NA")
						&& !arrivalDelay.contains("None")) {
					totalDelay += Integer.parseInt(arrivalDelay);

				}

				if (!departureDelay.contains("NA")
						&& !departureDelay.contains("None")) {
					totalDelay += Integer.parseInt(departureDelay);

				}

				customizedDataType.setTotalDelay(new IntWritable(totalDelay));
				customizedDataType.setCount(new IntWritable(1));
				if (scheduledDepartureTime.length() >= 3) {
					
					String hour = scheduledDepartureTime.substring(0, scheduledDepartureTime.length() - 2);;
					context.write(new Text("H" + hour), customizedDataType);
					context.write(new Text("W" + dayOfWeek), customizedDataType);
					context.write(new Text("M" + month), customizedDataType);
				}

			}

		}
	}
}
