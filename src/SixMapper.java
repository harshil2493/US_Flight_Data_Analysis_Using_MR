import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SixMapper extends
		Mapper<LongWritable, Text, Text, SixCustomizedDataType> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String valueString = value.toString();
		if (!valueString.startsWith("Year") && !valueString.startsWith("t")
				) {
			if (valueString.startsWith("N")) {
				String[] data = valueString.split(",");

				String tailNumber = data[0];
				String manufacturedYear = "";
				if (data.length == 9)
				{
					manufacturedYear = data[8];
					if(!manufacturedYear.equals("None"))
					{
					SixCustomizedDataType sixCustomizedDataType = new SixCustomizedDataType();
					sixCustomizedDataType.setFlag(new BooleanWritable(true));
					sixCustomizedDataType.setTotalDelay(new IntWritable());
					sixCustomizedDataType.setTotalCount(new IntWritable());
					sixCustomizedDataType.setDataLine(new IntWritable(Integer
							.parseInt(manufacturedYear)));
	
					context.write(new Text(tailNumber), sixCustomizedDataType);
					}
				}

			} else {
				String[] dataOfMainDataSet = valueString.split(",");

				String year = dataOfMainDataSet[0];

				String arrivalDelay = dataOfMainDataSet[14];
				String departureDelay = dataOfMainDataSet[15];

				int totalDelay = 0;

				String tailNumber = dataOfMainDataSet[10];
				if (!arrivalDelay.equals("NA")) {
					totalDelay = totalDelay + Integer.parseInt(arrivalDelay);
				}
				if (!departureDelay.equals("NA")) {
					totalDelay = totalDelay + Integer.parseInt(departureDelay);
				}

				SixCustomizedDataType sixCustomizedDataType = new SixCustomizedDataType();
				sixCustomizedDataType.setFlag(new BooleanWritable(false));
				sixCustomizedDataType
						.setTotalDelay(new IntWritable(totalDelay));
				sixCustomizedDataType.setTotalCount(new IntWritable(1));
				sixCustomizedDataType.setDataLine(new IntWritable(Integer
						.parseInt(year)));

				context.write(new Text(tailNumber), sixCustomizedDataType);

			}
		}
	}
}
