package com.lokihjl.mammon.producer;

import java.util.Properties;

import com.lokihjl.mammon.utils.StringUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

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

}
