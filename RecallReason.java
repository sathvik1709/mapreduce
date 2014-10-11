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

public class RecallReason extends Configured implements Tool {

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text cause = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString().toUpperCase();
			String[] data = line.split("\t");

			if (data[15].length() > 3) {
				String temp = data[2] + "-" + data[3] + " "
						+ data[15].substring(0, 4);
				word.set(temp);
				cause.set(data[6]);
				output.collect(word, cause);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		Text resultString = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String sum = "";
			while (values.hasNext()) {
				sum += " " + values.next().toString();
			}

			/*
			 * while (values.hasNext()) { sum += values.next().get(); }
			 */

			resultString.set(sum);
			output.collect(key, resultString);
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), RecallReason.class);
		conf.setJobName("TotalRecalls");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(1);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RecallReason(), args);
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