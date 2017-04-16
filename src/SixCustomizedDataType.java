import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SixCustomizedDataType implements Writable {

	IntWritable totalDelay;
	IntWritable totalCount;
	IntWritable dataLine;
	BooleanWritable flag;

	public SixCustomizedDataType() {
		// TODO Auto-generated constructor stub

		//
		totalDelay = new IntWritable();
		totalCount = new IntWritable();

		dataLine = new IntWritable();
		flag = new BooleanWritable();

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub

		totalDelay.readFields(dataInput);
		totalCount.readFields(dataInput);
		dataLine.readFields(dataInput);
		flag.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub

		totalDelay.write(dataOutput);
		totalCount.write(dataOutput);

		dataLine.write(dataOutput);
		flag.write(dataOutput);
		//

	}

	public IntWritable getTotalCount() {
		return totalCount;
	}

	public BooleanWritable getFlag() {
		return flag;
	}

	public void setTotalCount(IntWritable totalCount) {
		this.totalCount = totalCount;
	}

	public IntWritable getDataLine() {
		return dataLine;
	}

	public void setDataLine(IntWritable dataLine) {
		this.dataLine = dataLine;
	}

	public void setFlag(BooleanWritable flag) {
		this.flag = flag;
	}

	public IntWritable getTotalDelay() {
		return totalDelay;
	}

	public void setTotalDelay(IntWritable totalDelay) {
		this.totalDelay = totalDelay;
	}

}
