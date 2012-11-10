/**
 * Text Pair object class
 */
package edu.cse587.project.two.wordcooccur;

import java.util.HashMap;
import java.util.Set;

/**
 * @author suz
 *
 */
public class StripeMap {
	private HashMap<String, Integer> innerMap = new HashMap<String, Integer>(); 
	// constructor
	public StripeMap(){}

	// setter
	public void addup(String s){
		if(innerMap.containsKey(s)){ // if key exists, +1
			Integer ii = innerMap.get(s) + 1;
			innerMap.put(s, ii); // put a new value with same key
		} else { // else put a new key with count = 1;
			innerMap.put(s, new Integer(1));
		}
	}

	public void addup(String s, Integer i){
		if(innerMap.containsKey(s)){ // if key exists, +i
			Integer ii = innerMap.get(s) + i;
			innerMap.put(s, ii); // put a new value with same key
		} else { // else put a new key with count = i;
			innerMap.put(s, new Integer(i));
		}
	}

	// getter
	public Set<String> getKeySet(){
		return innerMap.keySet();
	}

	public Integer get(String s){
		return innerMap.get(s);
	}
	
	public int getCount(String s){
		return innerMap.get(s).intValue();
	}
}
