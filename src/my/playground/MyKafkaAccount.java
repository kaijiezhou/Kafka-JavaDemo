package my.playground;

/**
 * Created by kzhou on 10/31/16.
 */
public class MyKafkaAccount {
    //Account
    private String bootstrapServers = "localhost:9092";
    private String schemaRegistry = "localhost:18081";



    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    public String getSchemaRegistry() { return String.format("http://%s", schemaRegistry); }



}
