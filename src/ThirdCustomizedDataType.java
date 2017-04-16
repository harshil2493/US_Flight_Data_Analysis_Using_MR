import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ThirdCustomizedDataType implements Writable {

	Text dataLine;
	IntWritable count;

	public ThirdCustomizedDataType() {
		// TODO Auto-generated constructor stub

		//
		dataLine = new Text();
		count = new IntWritable();

	}

	public Text getDataLine() {
		return dataLine;
	}

	public void setDataLine(Text dataLine) {
		this.dataLine = dataLine;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub

		dataLine.readFields(dataInput);
		count.readFields(dataInput);

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub

		dataLine.write(dataOutput);
		count.write(dataOutput);
		//

	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

}
