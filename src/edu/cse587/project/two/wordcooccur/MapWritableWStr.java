package edu.cse587.project.two.wordcooccur;

import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MapWritableWStr extends MapWritable {
	public String toString(){
		Set<Writable> WStrKeySet = this.keySet();
		String result = ""; 
		for(Writable w : WStrKeySet){
			result = w.toString() + "\t" + this.get(w).toString();
		}
		return result;
	}
}
