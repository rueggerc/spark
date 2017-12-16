package com.rueggerllc.spark.functions;

public interface MyPredicate<T> {
	boolean isTrue(T t);
}
