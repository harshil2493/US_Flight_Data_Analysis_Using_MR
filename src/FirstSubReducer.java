import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstSubReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	private static HashMap sortByValues(Map<String, Float> argumentMap) {
		List list = new LinkedList(argumentMap.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return (((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue())) * 1;
			}
		});

		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		HashMap sortedHashMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Map<String, Float> dataForDay = new HashMap<String, Float>();

		Map<String, Float> dataForHour = new HashMap<String, Float>();
		Map<String, Float> dataForYear = new HashMap<String, Float>();

		for (Text all : values) {
			String[] read = all.toString().split("&");
			if (read[0].contains("H")) {
				dataForHour.put(read[0], Float.parseFloat(read[1]));
			} else if (read[0].contains("M")) {
				dataForYear.put(read[0], Float.parseFloat(read[1]));
			} else if (read[0].contains("W")) {
				dataForDay.put(read[0], Float.parseFloat(read[1]));
			}
		}

		Map<String, Float> sortDataForHour = sortByValues(dataForHour);
		Map<String, Float> sortDataForDay = sortByValues(dataForDay);
		Map<String, Float> sortDataForMonth = sortByValues(dataForYear);

		context.write(NullWritable.get(), new Text("For Question 1 & 2\n"));
		context.write(NullWritable.get(), new Text("**Hourly Best Order To Fly**"));
		for (String hour : sortDataForHour.keySet()) {
			context.write(NullWritable.get(), new Text("Hour: " + hour
					+ " Delay Observed: " + sortDataForHour.get(hour)));
		}

		context.write(NullWritable.get(), new Text("\n**Weekly Best Order To Fly**"));
		for (String week : sortDataForDay.keySet()) {
			context.write(NullWritable.get(), new Text("Hour: " + week
					+ " Delay Observed: " + sortDataForDay.get(week)));
		}

		context.write(NullWritable.get(), new Text("\n**Monthly Best Order To Fly**"));
		for (String month : sortDataForMonth.keySet()) {
			context.write(NullWritable.get(), new Text("Hour: " + month
					+ " Delay Observed: " + sortDataForMonth.get(month)));
		}

	}

}
