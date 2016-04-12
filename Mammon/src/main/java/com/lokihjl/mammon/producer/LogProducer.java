package com.lokihjl.mammon.producer;

import com.lokihjl.mammon.utils.StringUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.*;

/**
 * @author hjl
 * @E-mail:huangjl@neunn.com
 * @version 创建时间：2016年4月8日 下午5:16:43
 */
public class LogProducer {
    
    private Producer<String, String> inner ;
    
    public LogProducer() throws Exception{
        Properties properties = new Properties() ;
        
        properties.load(ClassLoader.getSystemResourceAsStream("producer.properties")); 
        ProducerConfig config = new ProducerConfig(properties) ;
        inner = new Producer<String, String>(config) ;
    }
    
    public void send(String topicName, String message) {
        if(StringUtils.isEmpty(topicName) || StringUtils.isEmpty(message)) {
            return ;
        }
        KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, message) ;
        
        inner.send(km);
    }

    public void send(String topicName, Collection<String> messages) {
        if(StringUtils.isEmpty(topicName) || messages == null || messages.isEmpty()) {
            System.out.print("return");
            return ;
        }
        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>() ;
        for(String entry: messages) {
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, entry) ;
            kms.add(km);
        }

        inner.send(kms);

    }

    public void close() {
        inner.close();
    }

    public static void main(String[] args) {
        LogProducer producer = null;
        try {
            producer = new LogProducer();
            int i = 0 ;
            while (true) {
                producer.send("test-topic", "this is a sample" + i);
                i++;
                sleep(2000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (producer != null) {
                producer.close();
            }
        }
    }


}
