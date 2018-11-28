package com.vdl.vdl.client;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by longdandan on 18/2/8.
 */
public class LoadTest extends Thread {

    private static String consumerType;
    private static String bootstrapServers;
    private static String logstream;
    private static int outputCount;

    private String threadName;
    private long consumeCount;

    public LoadTest(String name) {
        this.threadName = name;
    }


    @Override
    public void run() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<Integer, byte[]>(props);

        TopicPartition partition = new TopicPartition(logstream, 0);
        consumer.assign(Collections.singletonList(partition));

        consumer.seekToEnd(Collections.singletonList(partition));

        while (true) {

            try{
                if (consumerType.equals("r")) {
                    Map<TopicPartition, Long> endOffsetMap = consumer.endOffsets(Collections.singletonList(partition));
                    Map<TopicPartition, Long> beginOffsetMap = consumer.beginningOffsets(Collections.singletonList(partition));
                    Long endOffset = endOffsetMap.get(partition);
                    Long beginOffset = beginOffsetMap.get(partition);
                    Long wantOffset = Math.round(Math.random() * (endOffset - beginOffset)) + beginOffset;
                    consumer.seek(partition, wantOffset);
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                continue;
            }


            for (int i = 0; i < 30; i++) {
                ConsumerRecords<Integer, byte[]> records = null;
                try {
                    records = consumer.poll(1000);
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<Integer, byte[]> record : records) {
                            consumeCount++;
                            if (consumeCount % outputCount == 0) {
                                System.out.println(System.currentTimeMillis() + ": " + this.threadName + " consume msg count " + this.consumeCount);
                            }
                        }
                    } else {
                        break;
                    }
                } catch (RuntimeException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static CommandLine getCommandLine(String[] args) {
        Options options = new Options();

        Option consumeType = new Option("t", "type", true, "[r|n], r mean consume random offset, n mean consume newest");
        consumeType.setRequired(true);
        options.addOption(consumeType);

        Option threadCount = new Option("c", "threadCount", true, "how many thread to consume, note: each thread will have one consumer");
        threadCount.setRequired(true);
        options.addOption(threadCount);

        Option vdls = new Option("v", "vdls", true, "vdl server address, ip:port,ip:port");
        vdls.setRequired(true);
        options.addOption(vdls);

        Option logstream = new Option("l", "logstream", true, "logstream name");
        logstream.setRequired(true);
        options.addOption(logstream);

        Option outputCount = new Option("o", "outputCount", true, "how many record print tips");
        outputCount.setRequired(true);
        options.addOption(outputCount);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("LoadTest", options);
            System.exit(1);
            return null;
        }
        return cmd;
    }

    public static void main(String[] args) {

        CommandLine cmd = getCommandLine(args);

        consumerType = cmd.getOptionValue("type");
        bootstrapServers = cmd.getOptionValue("vdls");
        logstream = cmd.getOptionValue("logstream");
        outputCount = Integer.parseInt(cmd.getOptionValue("outputCount"));

        int threadCount = Integer.parseInt(cmd.getOptionValue("threadCount"));

        for (int i = 0; i < threadCount; i++) {
            LoadTest tester = new LoadTest("consumerThread-" + i);
            tester.start();
        }

        while (true) {
            try {
                Thread.sleep(100 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
