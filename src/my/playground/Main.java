package my.playground;

public class Main {

    public static void main(String[] args) throws Exception {
        kafkaDemo();
    }

    public static void kafkaDemo() throws Exception{
        MyKafkaAccount account = new MyKafkaAccount();
        MyProducer producer = new MyProducer(account);
        MyConsumer consumer = new MyConsumer(account, "test-group-id", MyConsumer.DataType.AVRO);

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        producer.run();
        consumerThread.join();
    }

//    public static void regexDemo() {
//        String regex = ".*CDH-([1-9]+\\.[1-9]+)\\.[1-9]+-.*";
//        Pattern ptn = Pattern.compile(regex);
//        Matcher m = ptn.matcher("CDH_SPARK_HOME=/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/spark");
//        System.out.println(m.matches());
//        if(m.find()) {
//            System.out.println(m.group(1));
//        }
//    }

}


