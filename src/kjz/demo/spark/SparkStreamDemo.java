package kjz.demo.spark;

import java.util.HashMap;
import java.util.HashSet;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

import kafka.serializer.StringDecoder;

public class SparkStreamDemo {
	//private String sourceHost;
	//private int sourcePort;
	private String topics;
	private String brokerList;
	public SparkStreamDemo(/*String sourceHost, int sourcePort,*/String brokerList,String topics){
		//this.sourceHost=sourceHost;
		//this.sourcePort=sourcePort;
		this.brokerList=brokerList;
		this.topics=topics;
	}
	public void runTerminalDemo(){
		SparkConf conf=new SparkConf().setAppName("SparkStreamDemo");
		JavaStreamingContext ctx=new JavaStreamingContext(conf, Durations.seconds(1));
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
	    HashMap<String, String> kafkaParams = new HashMap<String, String>();
	    kafkaParams.put("metadata.broker.list", brokerList);
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				ctx,
		        String.class,
		        String.class,
		        StringDecoder.class,
		        StringDecoder.class,
		        kafkaParams,
		        topicsSet
		    );
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	        @Override
	        public String call(Tuple2<String, String> tuple2) {
	          return tuple2._2();
	        }
	      });
	    JavaPairDStream<String, Integer> wordCounts = lines.mapToPair(
	    	      new PairFunction<String, String, Integer>() {
	    	        @Override
	    	        public Tuple2<String, Integer> call(String s) {
	    	          return new Tuple2<String, Integer>(s, 1);
	    	        }
	    	      }).reduceByKey(
	    	        new Function2<Integer, Integer, Integer>() {
	    	        @Override
	    	        public Integer call(Integer i1, Integer i2) {
	    	          return i1 + i2;
	    	        }
	    	      });
	    wordCounts.print();
	    ctx.start();
	    ctx.awaitTermination();
	}
}
