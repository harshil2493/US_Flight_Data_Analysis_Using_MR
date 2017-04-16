import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class FirstCustomizedDataType implements Writable {

	IntWritable totalDelay;
	IntWritable count;

	public FirstCustomizedDataType() {
		// TODO Auto-generated constructor stub

		//
		totalDelay = new IntWritable();
		count = new IntWritable();

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub

		totalDelay.readFields(dataInput);
		count.readFields(dataInput);

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub

		totalDelay.write(dataOutput);
		count.write(dataOutput);
		//

	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public IntWritable getTotalDelay() {
		return totalDelay;
	}

	public void setTotalDelay(IntWritable totalDelay) {
		this.totalDelay = totalDelay;
	}

}
