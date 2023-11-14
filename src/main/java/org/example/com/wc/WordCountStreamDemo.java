package org.example.com.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.readTextFile("input/words.txt");

        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        // normalize and split the line
                        String[] words = value.split(" ");

                        // emit the words
                        for (String word : words) {
                            if (word.length() > 0) {
                                out.collect(new Tuple2<>(word, 1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        counts.print();

        env.execute("Streaming WordCount");
    }
}
