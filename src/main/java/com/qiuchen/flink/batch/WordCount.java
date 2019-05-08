package com.qiuchen.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/** Word Count Demo
 *
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        String path = "/opt/flink-1.7.2/README.txt";

        //Create Flink batch environment context.
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //job source
        DataSource<String> text = env.readTextFile(path);


        //transformation
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new Tuple2(word, 1));
                }
            }
        }).groupBy(0)
          .sum(1)
          .print();


    }
}
