import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends
		Mapper<LongWritable, Text, Text, ThirdCustomizedDataType> {
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
				String airportName = data[3];

				ThirdCustomizedDataType customizedDataTypeForQ3 = new ThirdCustomizedDataType();
				customizedDataTypeForQ3.setCount(new IntWritable(0));
				customizedDataTypeForQ3.setDataLine(new Text(airportName));
				context.write(new Text(IATAName), customizedDataTypeForQ3);

			} else {
				String[] dataOfMainDataSet = valueString.split(",");
				String year = dataOfMainDataSet[0];
				String from = dataOfMainDataSet[16];
				String to = dataOfMainDataSet[17];

				ThirdCustomizedDataType customizedDataTypeForQ3 = new ThirdCustomizedDataType();
				customizedDataTypeForQ3.setCount(new IntWritable(1));
				customizedDataTypeForQ3.setDataLine(new Text(year));

				context.write(new Text(from), customizedDataTypeForQ3);
				context.write(new Text(to), customizedDataTypeForQ3);

			}
		}
	}
}
