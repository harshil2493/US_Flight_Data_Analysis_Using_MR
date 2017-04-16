import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FifthCustomizedDataType implements Writable {

	IntWritable totalDelay;
	IntWritable totalCount;
	Text dataLine;
	BooleanWritable flag;

	public FifthCustomizedDataType() {
		// TODO Auto-generated constructor stub

		//
		totalDelay = new IntWritable();
		totalCount = new IntWritable();

		dataLine = new Text();
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

	public Text getDataLine() {
		return dataLine;
	}

	public BooleanWritable getFlag() {
		return flag;
	}

	public void setTotalCount(IntWritable totalCount) {
		this.totalCount = totalCount;
	}

	public void setDataLine(Text dataLine) {
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
