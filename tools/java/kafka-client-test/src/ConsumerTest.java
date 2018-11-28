import encoding.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import rdp.messages.RDPProbuf;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by longdandan on 17/7/26.
 */
public class ConsumerTest extends Thread{

    public static final String TOPIC = "rdpbinlog";
    public static final String KAFKA_SERVER_URL = "192.168.0.1";
    public static final int KAFKA_SERVER_PORT = 7100;

    private KafkaConsumer<Integer, byte[]> consumer;
    private final String topic;

    private Properties props;

    public ConsumerTest(String topic) {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerTest.KAFKA_SERVER_URL + ":" + ConsumerTest.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "900");
//        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "true");
//        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "900");
//        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "100");
//        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "100");
//        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "400");

        this.topic = topic;
    }



    @Override
    public void run() {
        System.out.println("run");
        consumer = new KafkaConsumer<Integer, byte[]>(props);
        TopicPartition  partition = new TopicPartition(this.topic,0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seek(partition,0);
        while(true){
            ConsumerRecords<Integer, byte[]> records = null;
            try{
                records = consumer.poll(1000);
            }catch(RuntimeException e){
                System.out.println(e);
            }

            for (ConsumerRecord<Integer, byte[]> record : records) {
                System.out.println("Received message: (" + record.key() + ") at offset " + record.offset() + " crc:" + record.checksum());
                try {
                    Message.VMSMessage vmsMessage = Message.VMSMessage.parseFrom(record.value());
                    RDPProbuf.KafkaPkg pkg = RDPProbuf.KafkaPkg.parseFrom(vmsMessage.getPayload().toByteArray());
                    RDPProbuf.Transaction tran = RDPProbuf.Transaction.parseFrom(pkg.getData());
                    print(tran);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

    }


    public static void print(RDPProbuf.Transaction tran) {
        System.out.println("GTID:"  + tran.getGtid().toStringUtf8());
        for (int i = 0; i < tran.getEventsCount(); i++) {
            RDPProbuf.Event event = tran.getEvents(i);
            System.out.println("database: " + event.getDatabaseName().toStringUtf8() + " table: " + event.getTableName().toString());
            for (int j = 0; j < event.getRowsCount(); j++) {
                RDPProbuf.Row row = event.getRows(j);
                System.out.println("before row:");
                for (int k = 0; k < row.getBeforeCount(); k++) {
                    RDPProbuf.Column column = row.getBefore(k);
                    System.out.println("column name:" + column.getName().toStringUtf8() + " value:" + column.getValue().toStringUtf8());
                }
                System.out.println("after row");
                for (int k = 0; k < row.getAfterCount(); k++) {
                    RDPProbuf.Column column = row.getAfter(k);
                    System.out.println("column name:" + column.getName().toStringUtf8() + " value:" + column.getValue().toStringUtf8());
                }
            }
        }
    }



    public static void main(String[] args) {
//        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
//        Producer producerThread = new Producer(ConsumerTest.TOPIC, isAsync);
//        producerThread.start();

        ConsumerTest consumerThread = new ConsumerTest(ConsumerTest.TOPIC);
        consumerThread.start();

        try {
            Thread.sleep(10000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
