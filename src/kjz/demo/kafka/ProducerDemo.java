package kjz.demo.kafka;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ProducerDemo {
	private String brokerList;
	//private String msg;
	private Producer<String, String> producer;
	public ProducerDemo(String brokerList){
		this.brokerList=brokerList;
		Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
		//this.msg=msg;
	}
	public void runListenerDemo(String topic, String key,String msg){
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, msg);
        producer.send(data);
		
    }
	public void runGeneraterDemo(int n){
		Random rnd = new Random();
		System.out.println("Begin sending msges...");
        for (long nEvents = 0; nEvents < n; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "92.168.2." + rnd.nextInt(255); 
               String msg = runtime + ", www.example.com," + ip; 
               String topic="test"+rnd.nextInt(5);
               KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
               producer.send(data);
               System.out.println("[producer] Message sent : "+topic+", "+msg);
        }
        producer.close();
	}
	public void Close_Producer(){
		if (producer != null)
			producer.close();
	}
}
