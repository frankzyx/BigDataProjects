/*
MyWordCount.java
Jan 28, 2017
*/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import javax.security.auth.login.Configuration;

public class MyWordCount {

	public static class TokenMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		/*
		protected void map(KEYIN key,
                   VALUEIN value,
                   org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException,
                   InterruptedException
		*/
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
		/* Using StringTokenizer
			StringTokenizer st = new StringTokenizer(
				value.toString().replaceAll("[^a-zA-Z\\s+]", "").toLowerCase());
			while (st.hasMoreTokens()) {
				word.set(st.nextToken());    // set the Text to contain the contents of that string
				context.write(word, one);		
			}
		*/
			String[] words = value.toString().replaceAll("[^a-zA-Z\\s+]", "").toLowerCase().split("\\s+");
			for (String w : words) {
				word.set(w);
				context.write(word, one);
			}
		}
	}



	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		/*
		protected void reduce(KEYIN key,
	          Iterable<VALUEIN> values,
	          org.apache.hadoop.mapreduce.Reducer.ContextBy removing the rubbish characters before splitting, you avoid having to loop through the elements. context)
            throws IOException,
           		   InterruptedException
		*/
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException{
			// this method is called for each <key, (collections of values)> in the sorted inputs
			// AFTER the shuffle & sort step
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();		// get the value of IntWritable object
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MyWordCount.class);
		job.setMapperClass(TokenMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
