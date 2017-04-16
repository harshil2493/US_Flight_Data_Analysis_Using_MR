import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SevenMapper extends
		Mapper<LongWritable, Text, Text, SevenCustomizedDataType> {
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
			String lateAircraftDelay = (parsedValues[28]);

			String arrivalDelay = (parsedValues[14]);
			String departureDelay = (parsedValues[15]);
			
			String uniqueCarrierCode = parsedValues[8];
			
			if (!scheduledDepartureTime.contains("NA")) {
//				while (timeDepart.length() <= 4) {
//					timeDepart = "0" + timeDepart;
//				}
				//
//				scheduledDepartureTime = timeDepart;
				// String timeDepart = "";

				SevenCustomizedDataType customizedDataType = new SevenCustomizedDataType();

				// Question 1 & 2

				if (!lateAircraftDelay.contains("NA") && !lateAircraftDelay.contains("0")) {

					// Setting Arrival
					int totalDelay = Integer.parseInt(lateAircraftDelay);
					
					if(totalDelay !=0)
					{

					customizedDataType
							.setTotalDelay(new IntWritable(totalDelay));
					customizedDataType.setCount(new IntWritable(1));
					if (scheduledDepartureTime.length() >= 3) {
//						if(scheduledDepartureTime.length() == 3)
//							scheduledDepartureTime = "0" + scheduledDepartureTime;
						String hour = scheduledDepartureTime.substring(0, scheduledDepartureTime.length() - 2);
						context.write(new Text("H" + hour), customizedDataType);
						context.write(new Text("W" + dayOfWeek),
								customizedDataType);
						context.write(new Text("M" + month), customizedDataType);
					}
					int finalDelay = 0;
					if(!arrivalDelay.equals("NA"))
					{
						finalDelay = finalDelay + Integer.parseInt(arrivalDelay);
					}
					if(!departureDelay.equals("NA"))
					{
						finalDelay = finalDelay + Integer.parseInt(departureDelay);
					}
					if(finalDelay !=0)
					{
						float percentageWeight = (float)(totalDelay * 100.0f / finalDelay);
						customizedDataType.setPercent(new FloatWritable(percentageWeight));
						
						context.write(new Text("P"), customizedDataType);

					}
					
					context.write(new Text("B"+uniqueCarrierCode), customizedDataType);
					
					}

				}
			}
		}
	}
}
