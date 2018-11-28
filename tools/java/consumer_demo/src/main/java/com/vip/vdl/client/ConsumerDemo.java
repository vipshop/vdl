package com.vip.vdl.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import rdp.messages.RDPProbuf;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by longdandan on 18/2/8.
 */
public class ConsumerDemo {

    public static final String TOPIC = "rdpbinlog";
    public static final String BOOTSTRAP_SERVERS = "192.168.0.1:7100,192.168.0.1:7100,192.168.0.1:7100";

    private static KafkaConsumer<Integer, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<Integer, byte[]>(props);
        return consumer;
    }

    public static void main(String[] args) {

        KafkaConsumer<Integer, byte[]> consumer = createConsumer();

        // VDL topic only have one partition
        TopicPartition partition = new TopicPartition(TOPIC,0);
        consumer.assign(Collections.singletonList(partition));

        // set the offset to consume
        // set to beginning:
        //    consumer.seekToBeginning(Collections.singletonList(partition));
        // set to End(newest):
        //    consumer.seekToEnd(Collections.singletonList(partition));
        // set to special:
        consumer.seek(partition,5);

        while(true) {
            ConsumerRecords<Integer, byte[]> records = null;
            try {
                records = consumer.poll(1000);
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    System.out.println("Received message: (" + record.key() + ") at offset " + record.offset() + " crc:" + record.checksum());
                    try {

                        // data will package like:
                        // VMS Encode[ RDP Encode [data] ]

                        // so first decode the vms package
                        RDPProbuf.VMSMessage vmsMessage = RDPProbuf.VMSMessage.parseFrom(record.value());

                        // Decode RDP Package
                        // PLEASE NOTE:
                        // RDP may split one Transaction into multi-records (in RDP next version),
                        // in that scenario, please refer RDP to build multi record into one transaction.
                        // the blow demo is just for one record : one transaction
                        RDPProbuf.KafkaPkg pkg = RDPProbuf.KafkaPkg.parseFrom(vmsMessage.getPayload().toByteArray());
                        RDPProbuf.Transaction tran = RDPProbuf.Transaction.parseFrom(pkg.getData());
                        print(tran);

                        // save offset by yourself

                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            } catch (RuntimeException e) {
                System.out.println(e);
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

}
