import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FourthCustomizedDataType implements Writable {

	Text dataLine;
	BooleanWritable flag;

	public FourthCustomizedDataType() {
		// TODO Auto-generated constructor stub

		//
		dataLine = new Text();
		flag = new BooleanWritable();

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
		flag.readFields(dataInput);

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub

		dataLine.write(dataOutput);
		flag.write(dataOutput);
		//

	}

	public BooleanWritable getFlag() {
		return flag;
	}

	public void setFlag(BooleanWritable flag) {
		this.flag = flag;
	}

}
