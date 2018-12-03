import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by longdandan on 17/5/17.
 */
public class ProducerTest {

    public static String getRandomString(int length) {
        String KeyString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuffer sb = new StringBuffer();
        int len = KeyString.length();
        for (int i = 0; i < length; i++) {
            sb.append(KeyString.charAt((int) Math.round(Math.random() * (len - 1))));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws InterruptedException {

        String value = getRandomString(100 * 1024);

        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.0.1:8181");//vdl:8181, kafka:9191 //
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("compression.type", "none");
        props.put("acks", "-1");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("begin send:" + System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<String, String>("fiutopic", "key1234", value), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println(System.currentTimeMillis() + ", offset: " + metadata.offset() + " meta:" + metadata.toString() + " crc:" + metadata.checksum());
                }
            });
        }
        System.out.println("8888888:produce over");
        //Thread.sleep(1000000);
    }
}
