package health;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class count {
	public static class Mypartition extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text arg0, IntWritable arg1, int arg2) {
			// TODO Auto-generated method stub
			String key = arg0.toString();
			if (key.equalsIgnoreCase("Diabetes")) {
				return 0;
			} else if (key.equalsIgnoreCase("Fever")) {
				return 1;
			} else if (key.equalsIgnoreCase("Blood Pressure")) {
				return 2;
			} else if (key.equalsIgnoreCase("Malaria")) {
				return 3;
			} else if (key.equalsIgnoreCase("PCOS")) {
				return 4;
			} else if (key.equalsIgnoreCase("Swine Flu")) {
				return 5;
			} else if (key.equalsIgnoreCase("Cold")) {
				return 6;
			} else {
				return 7;
			}
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			for (String word : line.split(",")) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
				for (IntWritable i : value) {
					count += i.get();
				}
				context.write(key, new IntWritable(count));
			
			
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "my word count with partition");
		job.setJarByClass(count.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		job.setNumReduceTasks(8);
		job.setPartitionerClass(Mypartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job,
		// new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
