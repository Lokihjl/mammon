package com.lokihjl.mammon.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Loki on 16/4/11.
 */
public class LogConsumer {

    private ConsumerConfig config;
    private String topic ;
    private int partitionsNum ;
    private MessageExecutor executor ;
    private ConsumerConnector connector ;
    private ExecutorService threadPool ;

    public LogConsumer(String topic,int partitionsNum,MessageExecutor executor) throws Exception{
        Properties properties = new Properties() ;
        properties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
        config = new ConsumerConfig(properties) ;
        this.topic = topic ;
        this.partitionsNum = partitionsNum ;
        this.executor = executor ;
    }

    public void start() throws Exception {
        connector = Consumer.createJavaConsumerConnector(config) ;
        Map<String, Integer> topics = new HashMap<String, Integer>() ;
        topics.put(topic, partitionsNum) ;
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics) ;
        List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
        threadPool = Executors.newFixedThreadPool(partitionsNum);
        for(KafkaStream<byte[], byte[]> partition : partitions){
            threadPool.execute(new MessageRunner(partition));
        }
    }

    public void close() {
        try{
            threadPool.shutdown();
        }catch (Exception e){

        }finally {
            connector.shutdown();
        }
    }

    class MessageRunner implements Runnable{
        private KafkaStream<byte[], byte[]> partition ;

        MessageRunner(KafkaStream<byte[], byte[]> partition) {
            this.partition = partition ;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = partition.iterator() ;
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> item = it.next() ;
                System.out.print("partition:" + item.partition());
                System.out.print("offset:" + item.offset());
                executor.execute(new String(item.message()));
            }
        }
    }


    interface MessageExecutor {
        public void execute(String message) ;
    }

    public static void main(String[] args) {
        LogConsumer log = null ;
        try{
            MessageExecutor executor = new MessageExecutor() {
                @Override
                public void execute(String message) {
                    System.out.println(message) ;
                }
            };
        }catch(Exception e) {
            e.printStackTrace();
        }finally {

        }
    }
}
