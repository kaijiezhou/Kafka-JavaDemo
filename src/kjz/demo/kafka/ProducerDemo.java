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
	public void runDemo(String topic, String key,String msg){
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, msg);
        producer.send(data);
		//Random rnd = new Random();
        /*for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "92.168.2." + rnd.nextInt(255); 
               String msg = runtime + ", www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
               producer.send(data);
        }*/
        //producer.close();
    }
	public void Close_Producer(){
		if (producer != null)
			producer.close();
	}
}
