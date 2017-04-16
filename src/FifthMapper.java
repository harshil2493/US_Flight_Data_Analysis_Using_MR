import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FifthMapper extends
		Mapper<LongWritable, Text, Text, FifthCustomizedDataType> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String valueString = value.toString();
		if (!valueString.startsWith("Year") && !valueString.startsWith("Code")) {
			if (valueString.startsWith("\"")) {
				String[] data = valueString.split("\"");
				String carrierCode = data[1];
				String carrierName = data[3];

				FifthCustomizedDataType fifthCustomizedDataType = new FifthCustomizedDataType();
				fifthCustomizedDataType.setFlag(new BooleanWritable(true));
				fifthCustomizedDataType.setTotalDelay(new IntWritable());
				fifthCustomizedDataType.setTotalCount(new IntWritable());
				fifthCustomizedDataType.setDataLine(new Text(carrierName));

				context.write(new Text(carrierCode), fifthCustomizedDataType);

			} else {
				String[] dataOfMainDataSet = valueString.split(",");

				String uniqueCarrierCode = dataOfMainDataSet[8];

				String arrivalDelay = dataOfMainDataSet[14];
				String departureDelay = dataOfMainDataSet[15];

				int totalDelay = 0;
				if (!arrivalDelay.equals("NA")) {
					totalDelay = totalDelay + Integer.parseInt(arrivalDelay);
				}
				if (!departureDelay.equals("NA")) {
					totalDelay = totalDelay + Integer.parseInt(departureDelay);
				}
				if (totalDelay > 0) {
					FifthCustomizedDataType fifthCustomizedDataType = new FifthCustomizedDataType();
					fifthCustomizedDataType.setFlag(new BooleanWritable(false));
					fifthCustomizedDataType.setTotalDelay(new IntWritable(
							totalDelay));
					fifthCustomizedDataType.setTotalCount(new IntWritable(1));
					fifthCustomizedDataType.setDataLine(new Text(""));
					context.write(new Text(uniqueCarrierCode),
							fifthCustomizedDataType);

				}
			}
		}
	}
}
