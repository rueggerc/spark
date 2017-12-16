package com.rueggerllc.spark.functions;

import java.io.Serializable;

@FunctionalInterface
public interface MyMappingFunction<T,R> extends Serializable {
	R mapIt(T t);
}
