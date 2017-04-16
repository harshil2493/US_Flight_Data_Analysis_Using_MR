import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FourthMapper extends
		Mapper<LongWritable, Text, Text, FourthCustomizedDataType> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String valueString = value.toString();
		if (!valueString.startsWith("Year")
				&& !valueString.startsWith("\"iata\"")) {
			if (valueString.startsWith("\"")) {
				String[] data = valueString.split("\"");
				String IATAName = data[1];
				String airportCity = data[5];

				FourthCustomizedDataType fourthCustomizedDataType = new FourthCustomizedDataType();
				fourthCustomizedDataType.setFlag(new BooleanWritable(true));
				fourthCustomizedDataType.setDataLine(new Text(airportCity));
				context.write(new Text(IATAName), fourthCustomizedDataType);

			} else {
				String[] dataOfMainDataSet = valueString.split(",");

				String from = dataOfMainDataSet[16];
				String to = dataOfMainDataSet[17];

				String weatherDelay = dataOfMainDataSet[25];

				FourthCustomizedDataType fourthCustomizedDataType = new FourthCustomizedDataType();
				fourthCustomizedDataType.setFlag(new BooleanWritable(false));
				fourthCustomizedDataType.setDataLine(new Text(weatherDelay));
				if (!weatherDelay.equals("NA") && !weatherDelay.equals("0")) {
					context.write(new Text(from), fourthCustomizedDataType);
					context.write(new Text(to), fourthCustomizedDataType);
				}
			}
		}
	}
}
