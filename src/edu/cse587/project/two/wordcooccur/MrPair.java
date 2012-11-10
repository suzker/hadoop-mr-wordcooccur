/**
 * Implementation of pair style word co-occurance.
 */
package edu.cse587.project.two.wordcooccur;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author suz
 * @date Dec 1st, 2011
 */
public class MrPair {
	public static class PairMapper 
	extends Mapper<Object, Text, Text, IntWritable>{//Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>

		private final static IntWritable one = new IntWritable(1);
		private Text term = new Text();
		
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String buff = null;
			
			while (itr.hasMoreTokens()) { // record terms
				term.set(itr.nextToken());
				if (buff!=null){
					Text keyEmit = new Text("<"+ buff +"\t"+ term.toString() + ">");
					context.write(keyEmit, one);
				}
				buff = term.toString();
			}
		}
	}

	public static class PairReducer 
	extends Reducer<Text,IntWritable,Text,IntWritable> {//Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
				Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
