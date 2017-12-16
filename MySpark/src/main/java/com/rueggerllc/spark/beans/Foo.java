package com.rueggerllc.spark.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.rueggerllc.spark.functions.MyListener;
import com.rueggerllc.spark.functions.MyMappingFunction;

public class Foo {
	
	public void doSomething(MyListener listener, int x) {
		listener.doSomeWork(x);
	}
	
	public static void runTheFunction(Function<String,String> theFunction, List<String> inputs) {
		for (String next : inputs) {
			String foo = theFunction.apply(next);
			System.out.println("Foo=" + foo);
		}
	}
	
	
	public List<Integer> runIt(MyMappingFunction<String,Integer> theFunction, List<String> inputs) {
		List<Integer> results = new ArrayList<Integer>();
		for (String next : inputs) {
			Integer result = theFunction.mapIt(next);
			results.add(result);
		}
		return results;
	}


}
