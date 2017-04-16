import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MasterJob {
	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		String outputPath;
		boolean flagToRunFirstPhase = true;
		boolean flagToRunThird = true;
		boolean flagToRunFourth = true;
		boolean flagToRunFifth = true;
		boolean flagToRunSix = true;
		boolean flagToRunSeven = true;
		
		System.out.println("Phase One: " + flagToRunFirstPhase);

		if (flagToRunFirstPhase) {
			Job job = Job.getInstance(conf, "Analysis_hkshah");

			job.setJarByClass(MasterJob.class);

			job.setMapperClass(FirstMapper.class);

			job.setCombinerClass(FirstCombiner.class);

			job.setReducerClass(FirstReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FirstCustomizedDataType.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_First_Temp";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);

			Job jobSub = Job.getInstance(conf, "Analysis_hkshah");

			jobSub.setJarByClass(MasterJob.class);

			jobSub.setMapperClass(FirstSubMapper.class);

			// job.setPartitionerClass(FirstPartitioner.class);

			jobSub.setReducerClass(FirstSubReducer.class);

			jobSub.setMapOutputKeyClass(NullWritable.class);
			jobSub.setMapOutputValueClass(Text.class);

			jobSub.setOutputKeyClass(NullWritable.class);
			jobSub.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSub, new Path(outputPath));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_First";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobSub, new Path(outputPath));
			jobSub.waitForCompletion(true);
		}

		else {
			outputPath = args[0];
		}
		System.out.println("Phase Three: " + flagToRunThird);

		if (flagToRunThird) {
			Job jobQ3 = Job.getInstance(conf, "Analysis_hkshah");

			jobQ3.setJarByClass(MasterJob.class);

			jobQ3.setMapperClass(ThirdMapper.class);

			jobQ3.setCombinerClass(ThirdCombiner.class);

			jobQ3.setReducerClass(ThirdReducer.class);

			jobQ3.setMapOutputKeyClass(Text.class);
			jobQ3.setMapOutputValueClass(ThirdCustomizedDataType.class);

			jobQ3.setOutputKeyClass(NullWritable.class);
			jobQ3.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobQ3, new Path(args[0]));
			FileInputFormat.addInputPath(jobQ3, new Path(args[1]
					+ "airports.csv"));

			outputPath = args[2] + "_Third_Temp";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobQ3, new Path(outputPath));
			jobQ3.waitForCompletion(true);

			Job jobSubQ3 = Job.getInstance(conf, "Analysis_hkshah");

			jobSubQ3.setJarByClass(MasterJob.class);

			jobSubQ3.setMapperClass(ThirdSubMapper.class);

			// job.setPartitionerClass(FirstPartitioner.class);

			jobSubQ3.setReducerClass(ThirdSubReducer.class);

			jobSubQ3.setMapOutputKeyClass(NullWritable.class);
			jobSubQ3.setMapOutputValueClass(Text.class);

			jobSubQ3.setOutputKeyClass(NullWritable.class);
			jobSubQ3.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSubQ3, new Path(outputPath));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_Third";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobSubQ3, new Path(outputPath));
			jobSubQ3.waitForCompletion(true);

		}

		else {
			outputPath = args[0];
		}

		System.out.println("Phase Four: " + flagToRunFourth);

		if (flagToRunFourth) {
			Job jobQ4 = Job.getInstance(conf, "Analysis_hkshah");

			jobQ4.setJarByClass(MasterJob.class);

			jobQ4.setMapperClass(FourthMapper.class);

			// jobQ4.setCombinerClass(FourthCombiner.class);

			jobQ4.setReducerClass(FourthReducer.class);

			jobQ4.setMapOutputKeyClass(Text.class);
			jobQ4.setMapOutputValueClass(FourthCustomizedDataType.class);

			jobQ4.setOutputKeyClass(NullWritable.class);
			jobQ4.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobQ4, new Path(args[0]));
			FileInputFormat.addInputPath(jobQ4, new Path(args[1]
					+ "airports.csv"));

			outputPath = args[2] + "_Fourth_Temp";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobQ4, new Path(outputPath));
			jobQ4.waitForCompletion(true);

			Job jobSubQ4 = Job.getInstance(conf, "Analysis_hkshah");

			jobSubQ4.setJarByClass(MasterJob.class);

			jobSubQ4.setMapperClass(FourthSubMapper.class);

			// job.setPartitionerClass(FirstPartitioner.class);

			jobSubQ4.setReducerClass(FourthSubReducer.class);

			jobSubQ4.setMapOutputKeyClass(NullWritable.class);
			jobSubQ4.setMapOutputValueClass(Text.class);

			jobSubQ4.setOutputKeyClass(NullWritable.class);
			jobSubQ4.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSubQ4, new Path(outputPath));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_Fourth";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobSubQ4, new Path(outputPath));
			jobSubQ4.waitForCompletion(true);

		}

		else {
			outputPath = args[0];
		}

		System.out.println("Phase Five: " + flagToRunFifth);

		if (flagToRunFifth) {
			Job jobQ5 = Job.getInstance(conf, "Analysis_hkshah");

			jobQ5.setJarByClass(MasterJob.class);

			jobQ5.setMapperClass(FifthMapper.class);

			jobQ5.setCombinerClass(FifthCombiner.class);

			jobQ5.setReducerClass(FifthReducer.class);

			jobQ5.setMapOutputKeyClass(Text.class);
			jobQ5.setMapOutputValueClass(FifthCustomizedDataType.class);

			jobQ5.setOutputKeyClass(NullWritable.class);
			jobQ5.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobQ5, new Path(args[0]));
			FileInputFormat.addInputPath(jobQ5, new Path(args[1]
					+ "carriers.csv"));

			outputPath = args[2] + "_Fifth_Temp";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobQ5, new Path(outputPath));
			jobQ5.waitForCompletion(true);

			Job jobSubQ5 = Job.getInstance(conf, "Analysis_hkshah");

			jobSubQ5.setJarByClass(MasterJob.class);

			jobSubQ5.setMapperClass(FifthSubMapper.class);

			// job.setPartitionerClass(FirstPartitioner.class);

			jobSubQ5.setReducerClass(FifthSubReducer.class);

			jobSubQ5.setMapOutputKeyClass(NullWritable.class);
			jobSubQ5.setMapOutputValueClass(Text.class);

			jobSubQ5.setOutputKeyClass(NullWritable.class);
			jobSubQ5.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSubQ5, new Path(outputPath));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_Fifth";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobSubQ5, new Path(outputPath));
			jobSubQ5.waitForCompletion(true);

		}

		else {
			outputPath = args[0];
		}
		System.out.println("Phase Six: " + flagToRunSix);

		if (flagToRunSix) {
			Job jobQ6 = Job.getInstance(conf, "Analysis_hkshah");

			jobQ6.setJarByClass(MasterJob.class);

			jobQ6.setMapperClass(SixMapper.class);

			jobQ6.setCombinerClass(SixCombiner.class);

			jobQ6.setReducerClass(SixReducer.class);

			jobQ6.setMapOutputKeyClass(Text.class);
			jobQ6.setMapOutputValueClass(SixCustomizedDataType.class);

			jobQ6.setOutputKeyClass(NullWritable.class);
			jobQ6.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobQ6, new Path(args[0]));
			FileInputFormat.addInputPath(jobQ6, new Path(args[1]
					+ "plane-data.csv"));

			outputPath = args[2] + "_Six_Temp";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobQ6, new Path(outputPath));
			jobQ6.waitForCompletion(true);

			Job jobSubQ6 = Job.getInstance(conf, "Analysis_hkshah");

			jobSubQ6.setJarByClass(MasterJob.class);

			jobSubQ6.setMapperClass(SixSubMapper.class);

			// job.setPartitionerClass(FirstPartitioner.class);

			jobSubQ6.setReducerClass(SixSubReducer.class);

			jobSubQ6.setMapOutputKeyClass(NullWritable.class);
			jobSubQ6.setMapOutputValueClass(Text.class);

			jobSubQ6.setOutputKeyClass(NullWritable.class);
			jobSubQ6.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSubQ6, new Path(outputPath));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_Six";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobSubQ6, new Path(outputPath));
			jobSubQ6.waitForCompletion(true);

		}

		else {
			outputPath = args[0];
		}
		System.out.println("Phase Seven: " + flagToRunSeven);

		if (flagToRunSeven) {
			Job jobQ7 = Job.getInstance(conf, "Analysis_hkshah");

			jobQ7.setJarByClass(MasterJob.class);

			jobQ7.setMapperClass(SevenMapper.class);

			jobQ7.setCombinerClass(SevenCombiner.class);

			jobQ7.setReducerClass(SevenReducer.class);

			jobQ7.setMapOutputKeyClass(Text.class);
			jobQ7.setMapOutputValueClass(SevenCustomizedDataType.class);

			jobQ7.setOutputKeyClass(NullWritable.class);
			jobQ7.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobQ7, new Path(args[0]));
//			FileInputFormat.addInputPath(jobQ7, new Path(args[1]
//					+ "plane-data.csv"));

			outputPath = args[2] + "_Seven_Temp";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobQ7, new Path(outputPath));
			jobQ7.waitForCompletion(true);

			Job jobSubQ7 = Job.getInstance(conf, "Analysis_hkshah");

			jobSubQ7.setJarByClass(MasterJob.class);

			jobSubQ7.setMapperClass(SevenSubMapper.class);

			// job.setPartitionerClass(FirstPartitioner.class);

			jobSubQ7.setReducerClass(SevenSubReducer.class);

			jobSubQ7.setMapOutputKeyClass(NullWritable.class);
			jobSubQ7.setMapOutputValueClass(Text.class);

			jobSubQ7.setOutputKeyClass(NullWritable.class);
			jobSubQ7.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSubQ7, new Path(outputPath));
			// FileInputFormat.addInputPath(job, new Path(args[1]));

			outputPath = args[2] + "_Seven";

			if (fileSystem.exists(new Path(outputPath)))
				fileSystem.delete(new Path(outputPath), true);

			FileOutputFormat.setOutputPath(jobSubQ7, new Path(outputPath));
			jobSubQ7.waitForCompletion(true);

		}

		else {
			outputPath = args[0];
		}

	}
}
