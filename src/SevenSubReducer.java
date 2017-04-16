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

public class SevenSubReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	private static HashMap sortByValues(Map<String, Float> argumentMap) {
		List list = new LinkedList(argumentMap.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return (((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue())) * -1;
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
		
		context.write(nullWrite, new Text("Question 7\nHarnessing LateAirCraftDelay"));
		Map<String, Float> dataForDay = new HashMap<String, Float>();

		Map<String, Float> dataForHour = new HashMap<String, Float>();
		Map<String, Float> dataForYear = new HashMap<String, Float>();
Map<String, Float> affectedCarrier = new HashMap<String, Float>();
		for (Text all : values) {
			String[] read = all.toString().split("&");
			if (read[0].startsWith("H")) {
				dataForHour.put(read[0], Float.parseFloat(read[1]));
			} else if (read[0].startsWith("M")) {
				dataForYear.put(read[0], Float.parseFloat(read[1]));
			} else if (read[0].startsWith("W")) {
				dataForDay.put(read[0], Float.parseFloat(read[1]));
			}
			else if (read[0].startsWith("B")) {
			
//				context.write(nullWrite, all);
				String carrier = read[0].substring(1);
				affectedCarrier.put(carrier, Float.parseFloat(read[1]));
			}
			else
			{
				
				context.write(nullWrite, new Text("\n**Percentage Tendency**\n"+all.toString()));
			}
		}

		Map<String, Float> sortDataForHour = sortByValues(dataForHour);
		Map<String, Float> sortDataForDay = sortByValues(dataForDay);
		Map<String, Float> sortDataForMonth = sortByValues(dataForYear);
		Map<String, Float> sortDataForCarrier = sortByValues(affectedCarrier);

//		context.write(NullWritable.get(), new Text("For Question 1 & 2"));
		context.write(NullWritable.get(), new Text("**Hourly Worst Order**"));
		for (String hour : sortDataForHour.keySet()) {
			context.write(NullWritable.get(), new Text("Hour: " + hour
					+ " Delay: " + sortDataForHour.get(hour)));
		}

		context.write(NullWritable.get(), new Text("\n**Weekly Worst Order**"));
		for (String week : sortDataForDay.keySet()) {
			context.write(NullWritable.get(), new Text("Week Day: " + week
					+ " Delay: " + sortDataForDay.get(week)));
		}

		context.write(NullWritable.get(), new Text("\n**Monthly Worst Order**"));
		for (String month : sortDataForMonth.keySet()) {
			context.write(NullWritable.get(), new Text("Month: " + month
					+ " Delay: " + sortDataForMonth.get(month)));
		}
		
		String answer = "\n**10 Affected Carriers**\n";
		int count = 10;
		for(String c : sortDataForCarrier.keySet())
		{
			answer = answer + c + " - ";
			count--;
			if(count==0)
			{
				break;
			}
		}
		context.write(NullWritable.get(), new Text(answer));

	}

}
