/*
Sathvik Shivaprakash
1000989203
CSE 6331 - Summer 2014
Programming Assignment 2 - Hadoop
*/
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalRecalls extends Configured implements Tool {

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString().toUpperCase();
			String[] data = line.split("\t");

		    String temp = data[14].toUpperCase();
			word.set(temp);
			output.collect(word, one);
			

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), TotalRecalls.class);
		conf.setJobName("TotalRecalls");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(1);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TotalRecalls(), args);
		System.exit(res);
	}
}


/*
References:
1) http://www-odi.nhtsa.dot.gov/downloads/folders/Recalls/Import_Instructions_Recalls.pdf
2) http://www-odi.nhtsa.dot.gov/downloads/folders/Recalls/RCL.txt
3) http://cs.smith.edu/dftwiki/index.php/Hadoop_WordCount.java
4) http://wiki.apache.org/hadoop/WordCount
5) http://hadoop.apache.org/docs/current/
6) https://www.youtube.com/watch?v=_qLTMpdP7H4
*/