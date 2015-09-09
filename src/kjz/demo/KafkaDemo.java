package kjz.demo;

import kjz.demo.kafka.ConsumerDemo;
import kjz.demo.kafka.ProducerDemo;

public class KafkaDemo {
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
