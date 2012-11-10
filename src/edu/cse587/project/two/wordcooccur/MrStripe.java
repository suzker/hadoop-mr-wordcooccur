/**
 * Implementation of stripe style word co-occurance.
 */
package edu.cse587.project.two.wordcooccur;

import edu.cse587.project.two.wordcooccur.StripeMap;
import edu.cse587.project.two.wordcooccur.MapWritableWStr; // use inheriented MapWritable class with toString() method to produce plain text output

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author suz
 * @date Dec 1st, 2011
 */
public class MrStripe {

	/**
	 * Mapper Class
	 */
	public static class StripeMapper 
	extends Mapper<Object, Text, Text, MapWritableWStr>{//Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
		@Override
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {

			//tokenization and write stripes map
			Map<String, StripeMap> termMap = new HashMap<String, StripeMap>();
			String[] tokens = value.toString().split("[\\s+\\n\\t]");
			boolean flagExclude = false; 
			for(String i : tokens){ // double loop
				if (i.length() != 0){
					for (String j : tokens){ // double loop
						if (i.compareToIgnoreCase(j) == 0 && !flagExclude && j.length()!=0){
							flagExclude = true;
						} else if (j.length() != 0){
							if(!termMap.containsKey(i)){
								termMap.put(i, new StripeMap());
							}
							termMap.get(i).addup(j);
						}
					}
				}
			}

			//double traverse term - stripes 
			Set<String> outterKeySet = termMap.keySet();
			for(String OKS : outterKeySet){
				Set<String> innerKeySet = termMap.get(OKS).getKeySet();
				for(String IKS : innerKeySet){
					MapWritableWStr valueMap = new MapWritableWStr();
					valueMap.put(new Text(IKS), new IntWritable(termMap.get(OKS).get(IKS))); // construct stripes using MapWritableWStr
					context.write(new Text(OKS), valueMap); // emit Text-Stripe pair
				}
			}
		}
	}

	/**
	 * Reducer Class
	 */
	public static class StripeReducer 
	extends Reducer<Text,MapWritableWStr,Text,MapWritableWStr> {//Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
		@Override
		public void reduce(Text key, Iterable<MapWritableWStr> values, 
				Context context
		) throws IOException, InterruptedException {
			StripeMap smap = new StripeMap();
			int normFactor = 0;						// normalization factor
			for (MapWritableWStr val : values) {	// sum up all Map with same key
				Set<Writable> term2 = val.keySet();
				for (Writable k : term2) {
					Integer tempCount = Integer.valueOf(val.get(k).toString());
					smap.addup(k.toString(), tempCount);
					normFactor = normFactor + tempCount.intValue();
				}
			}

			Set<String> stripeKeySet = smap.getKeySet();
			for (String SKS : stripeKeySet){
				MapWritableWStr sumMap = new MapWritableWStr();
				float normScore = (float)(smap.get(SKS).intValue() )/ normFactor;
				sumMap.put(new Text(SKS), new FloatWritable(normScore)); // construct stripes using MapWritableWStr
				context.write(key, sumMap); // emit key - value pair to write
			}
		}
	}
}
