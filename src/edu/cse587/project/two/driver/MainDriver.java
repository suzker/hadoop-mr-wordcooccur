package edu.cse587.project.two.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.cse587.project.two.wordcooccur.MapWritableWStr;
import edu.cse587.project.two.wordcooccur.MrPair;
import edu.cse587.project.two.wordcooccur.MrPair.PairMapper;
import edu.cse587.project.two.wordcooccur.MrPair.PairReducer;
import edu.cse587.project.two.wordcooccur.MrStripe;
import edu.cse587.project.two.wordcooccur.MrStripe.StripeMapper;
import edu.cse587.project.two.wordcooccur.MrStripe.StripeReducer;

/**
 * @author suz
 *	Main driver for Mapper Reducer Word Coocurance
 */
public class MainDriver {

	/**
	 * @param args
	 * 	args[0]	Input Folder in HDFS
	 * 	args[1] Output Folder in HDFS
	 * 	args[2] Word Co-occurance couting strategy
	 * @throws Exception 
	 * @date Dec 1st, 2011
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out> <type>");
			System.err.println("	<type>pair:		pair strategy for word co-occurance counting.");
			System.err.println("	<type>stripe:	stripe strategy for word co-occurance counting.");
			System.exit(2);
		}

		Job job = new Job(conf, "word cooccurance");

		if(otherArgs[2].equalsIgnoreCase("pair")){
			job.setJarByClass(MrPair.class);
			job.setMapperClass(PairMapper.class);
			job.setCombinerClass(PairReducer.class);
			job.setReducerClass(PairReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
		} else if(otherArgs[2].equalsIgnoreCase("stripe")){
			job.setJarByClass(MrStripe.class);
			job.setMapperClass(StripeMapper.class);
			job.setCombinerClass(StripeReducer.class);
			job.setReducerClass(StripeReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritableWStr.class);
		} else {
			System.err.println("Invalid job type!");
			System.err.println("Usage: wordcount <in> <out> <type>");
			System.err.println("	<type>pair:		pair strategy for word co-occurance counting.");
			System.err.println("	<type>stripe:	stripe strategy for word co-occurance counting.");
			System.exit(2);
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
