import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdSubReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	NullWritable nullWrite = NullWritable.get();

	private static HashMap sortByValues(Map<String, Integer> argumentMap) {
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
		Map<String, Integer> totalCounterToAirport = new HashMap<String, Integer>();

		Map<Integer, HashMap<String, Integer>> yearToAirportBusy = new TreeMap<Integer, HashMap<String, Integer>>();
		context.write(NullWritable.get(), new Text("Question 3\n\n**Yearly Data**"));
		context.write(NullWritable.get(), new Text("Year\tList Of Top Busiest Cities"));

		for (Text value : values) {
			String valueString = value.toString();
			String[] dataSplit = valueString.split("#");
			String[] airportAndFullName = dataSplit[0].split("==");
			String airport = airportAndFullName[0];
			Integer frequency = Integer.parseInt(dataSplit[1]);
			if (airportAndFullName.length != 1) {
				totalCounterToAirport.put(
						airport + "-" + airportAndFullName[1], frequency);
			} else {
				totalCounterToAirport.put(airport, frequency);

			}
			String[] yearsData = dataSplit[2].split("@");

			for (String dataStringToProcess : yearsData) {
				String[] yearAndCount = dataStringToProcess.split("&&&");
				int year = Integer.parseInt(yearAndCount[0]);
				int counts = Integer.parseInt(yearAndCount[1]);

				if (yearToAirportBusy.containsKey(year)) {
					yearToAirportBusy.get(year).put(airport, counts);
				} else {
					HashMap<String, Integer> airportToCount = new HashMap<String, Integer>();
					airportToCount.put(airport, counts);
					yearToAirportBusy.put(year, airportToCount);
				}

			}

		}
		for (Integer years : yearToAirportBusy.keySet()) {

			HashMap<String, Integer> sortedAirportCount = sortByValues(yearToAirportBusy
					.get(years));

			int countToPrint = 10;
			String answerString = years + "\t";
			for (String airports : sortedAirportCount.keySet()) {
				answerString = answerString + airports;
				countToPrint--;
				if (countToPrint == 0) {
					break;
				}
				else
				{
					answerString = answerString +   "-";
				}
			}

			context.write(NullWritable.get(), new Text(answerString));

		}

		HashMap<String, Integer> sortedAirportCountFinal = sortByValues(totalCounterToAirport);

		int countToPrint = 10;
		String answerString = "\n\n**Final Aggregated List**\n";
		for (String airports : sortedAirportCountFinal.keySet()) {
			answerString = answerString + airports + "\n";
			countToPrint--;
			if (countToPrint == 0) {
				break;
			}
		}

		context.write(NullWritable.get(), new Text(answerString));

	}

}
