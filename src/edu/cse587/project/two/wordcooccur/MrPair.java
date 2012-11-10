/**
 * Implementation of pair style word co-occurance.
 */
package edu.cse587.project.two.wordcooccur;

import java.io.IOException;
import java.lang.Math;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author suz
 * @date Dec 1st, 2011
 */
public class MrPair {
	public static class PairMapper 
	extends Mapper<Object, Text, Text, IntWritable>{//Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>

		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {

			String[] tokens = value.toString().split("[\\s+\\n\\t]");
			boolean flagExclude = false; 
			for(String i : tokens){ // double loop
				if (i.length() != 0){
					for (String j : tokens){ // double loop
						if (i.compareToIgnoreCase(j) == 0 && !flagExclude && j.length() != 0){
							flagExclude = true;
						} else if(j.length() != 0){
							Text keyEmit = new Text(i +"\t"+ j);
							context.write(keyEmit, one);						
						}
					}
					flagExclude = false;
				}
			}
		}
	}

	public static class PairPartioner
	extends Partitioner<Text, IntWritable> { //Class Partioner<KEYIN, VALUEIN>, from mapper
		private int numRedJob = 4;	// must be consistance with job.setNumReduceTasks(4); in MainDriver

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks){
			String[] keyParts = key.toString().split("[\\t]"); 
			return Math.abs(keyParts[0].hashCode() % numRedJob);
		}
	}

	public static class PairReducer 
	extends Reducer<Text,IntWritable,Text,FloatWritable> {//Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>

		private String keyWi=""; // preserve key w_i
		private Map<String, Integer> wjMap= new HashMap<String, Integer>();
		private int normFactor = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, 
				Context context
		) throws IOException, InterruptedException {

			String[] textSplit = key.toString().split("[\\t]");

			if(textSplit[0].compareTo(keyWi.toString()) == 0){ // compare the left key
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				wjMap.put(textSplit[1], new Integer(sum));//map this w_j - value pair
				normFactor += sum; // add up the norm factor
			} else { // if left key don't match the one we preserve, start emitting
				EmitResult(context);
				keyWi = textSplit[0]; //change the preserve key to a new one
			}
		}

		/*
		 * emit results 
		 */
		private void EmitResult(Context context) throws IOException, InterruptedException{
			// emit
			Set<String> wjSet = wjMap.keySet();
			for (String wj : wjSet){
				// emit the sum up value
				float normScore = (float)(wjMap.get(wj).intValue()) / normFactor;
				context.write(new Text(keyWi + "\t" + wj), new FloatWritable(normScore));
			}
			// clear up map and the w_i key, w_j - value pair mapping and the normFactor
			wjMap.clear();
			normFactor = 0;
		}
	}
}
