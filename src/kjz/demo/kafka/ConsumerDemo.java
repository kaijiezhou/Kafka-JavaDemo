package kjz.demo.kafka;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import scala.reflect.internal.Trees.This;
import scala.tools.nsc.backend.WorklistAlgorithm;

import java.awt.Robot;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerDemo {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	public ConsumerDemo(String a_zookeeper,String a_groupID, String a_topic){
		consumer=kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,a_groupID));
		this.topic=a_topic;
	}
	private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupID) {
		Properties prop=new Properties();
		prop.put("zookeeper.connect", a_zookeeper);
		prop.put("group.id", a_groupID);
		prop.put("zookeeper.session.timeout.ms", "400");
		prop.put("zookeeper.sync.time.ms", "200");
		prop.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(prop);
	}
	public void shutdown(){
		if(consumer!=null) consumer.shutdown();
		if(executor!=null)	executor.shutdown(); 
		try{
			if(!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)){
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		}catch(InterruptedException e){
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}
	
	public void run(int a_numThreads){
		Map<String, Integer> topicCountMap=new HashMap<>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap=consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams=consumerMap.get(topic);
		/*
		 * Launch all the threads
		 * */
		executor=Executors.newFixedThreadPool(a_numThreads);
		/*
		 * Create an object to consume the message
		 * */
		int threadNum=0;
		for(final KafkaStream stream:streams){
			executor.submit(new MultiThreadConsumer(stream, threadNum));
		}
	}
	public void runSingleConsumer(){
		Map<String, Integer> topicCountMap=new HashMap<>();
		topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap=consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams=consumerMap.get(topic);
		for(final KafkaStream stream:streams){
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while (it.hasNext())
	            System.out.println("Single Consumer:" + new String(it.next().message()));
	        System.out.println("Consuming done");
		}
		
	}
	
}
