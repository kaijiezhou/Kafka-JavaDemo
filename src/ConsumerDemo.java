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
			executor.submit(new ConsumerTest(stream, threadNum));
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
	public static void main(String[] args) {
		switch(args[0]){
		case "consumer":
			/* String zooKeeper = args[0];
	        String groupId = args[1];
	        String topic = args[2];*/
			String zooKeeper="172.17.0.4:3000,172.17.0.5:3000,172.17.0.6:3000";
			String groupId="group0";
			String topic="test";
	        //int threads = Integer.parseInt(args[3]);
	 
	        ConsumerDemo example = new ConsumerDemo(zooKeeper, groupId, topic);
	        //example.run(threads);
	        example.runSingleConsumer();
	 /*
	        try {
	            Thread.sleep(10000);
	        } catch (InterruptedException ie) {
	 
	        }
	 */
	        //example.shutdown();
	        break;
		case "producer":
			String msg=args[1];
			String brokerList="172.17.0.4:9092,172.17.0.5:9093,172.17.0.6:9094";
			ProducerDemo demo=new ProducerDemo(brokerList,msg);
			demo.runDemo(100);
			break;
		default:
			System.out.println("choose your modle!");
		}
       
    }
}
