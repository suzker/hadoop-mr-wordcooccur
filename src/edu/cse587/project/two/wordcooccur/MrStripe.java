/**
 * Implementation of stripe style word co-occurance.
 */
package edu.cse587.project.two.wordcooccur;

import edu.cse587.project.two.wordcooccur.StripeMap;
import edu.cse587.project.two.wordcooccur.MapWritableWStr; // use inheriented MapWritable class with toString() method to produce plain text output

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapWritable;
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
		private Text term = new Text();
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			//tokenization and write stripes map
			StringTokenizer itr = new StringTokenizer(value.toString());
			String buff = null;
			HashMap<String, StripeMap> termMap = new HashMap<String, StripeMap>();
			while (itr.hasMoreTokens()) { // record terms
				term.set(itr.nextToken());
				if (buff!=null){
					if(!termMap.containsKey(buff)){
						termMap.put(buff, new StripeMap());
					}
					termMap.get(buff).addup(term.toString());
				}
				buff = term.toString();
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

		public void reduce(Text key, Iterable<MapWritableWStr> values, 
				Context context
		) throws IOException, InterruptedException {
			StripeMap smap = new StripeMap();
			for (MapWritableWStr val : values) {	// sum up all Map with same key
				Set<Writable> term2 = val.keySet();
				for (Writable k : term2) {
					smap.addup(k.toString(), new Integer(val.get(k).toString()));
				}
			}
			
			Set<String> stripeKeySet = smap.getKeySet();
			for (String SKS : stripeKeySet){
				MapWritableWStr sumMap = new MapWritableWStr();
				sumMap.put(new Text(SKS), new IntWritable(smap.get(SKS))); // construct stripes using MapWritableWStr
				context.write(key, sumMap); // emit key - value pair to write
			}
		}
	}
}
