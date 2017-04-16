import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends
		Partitioner<Text, FirstCustomizedDataType> {
	@Override
	public int getPartition(Text key, FirstCustomizedDataType value,
			int numberOfReducers) {
		// TODO Auto-generated method stub

		return (Integer.parseInt(key.toString().substring(1, 5)) - 1987);
	}
}
