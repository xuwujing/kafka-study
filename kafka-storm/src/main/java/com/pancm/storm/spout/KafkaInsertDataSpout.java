package com.pancm.storm.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pancm.constant.Constants;


/**
 * 
* @Title: KafkaInsertDataSpout
* @Description: 
* 从kafka获取新增数据
* @Version:1.0.0  
* @author pancm
* @date 2018年4月19日
 */
public class KafkaInsertDataSpout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2548451744178936478L;
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaInsertDataSpout.class);
	
	private SpoutOutputCollector collector;
	
	private KafkaConsumer<String, String> consumer;
	
	private ConsumerRecords<String, String> msgList;
	
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map map, TopologyContext arg1, SpoutOutputCollector collector) {
		kafkaInit();
		this.collector = collector;
	}
	
	
	@Override
	public void nextTuple() {
		for (;;) {
			try {
				msgList = consumer.poll(100);
				if (null != msgList && !msgList.isEmpty()) {
					String msg = "";
					List<JSONObject> list=new ArrayList<JSONObject>();
					long tmpOffset=0;
					long maxOffset=0;
					for (ConsumerRecord<String, String> record : msgList) {
						// 原始数据
						msg = record.value();
						if (null == msg || "".equals(msg.trim())) {
							continue;
						}
						try{
							list.add(JSON.parseObject(msg));
						}catch(Exception e){
							logger.error("数据格式不符!数据:{}",msg);
							continue;
						}
						tmpOffset=record.offset();
						if(maxOffset<tmpOffset){
							maxOffset=tmpOffset;
						 }
				     } 
					logger.info("写入的数据:"+list);
					logger.info("消费的offset:"+maxOffset);
				   this.collector.emit(new Values(JSON.toJSONString(list)));
				   consumer.commitAsync();
				}else{
					TimeUnit.SECONDS.sleep(3);
					logger.info("未拉取到数据...");
				}
			} catch (Exception e) {
				logger.error("消息队列处理异常:", e);
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e1) {
					logger.error("暂停失败!",e1);
				}
			}
		}
		
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.FIELD));
	}
	
	/**
	 * 初始化kafka配置
	 */
	private void kafkaInit(){
		Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_SERVERS);  
        props.put("max.poll.records", 10);
        props.put("enable.auto.commit", false);
        props.put("group.id", "groupA");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        String topic=Constants.TOPIC_NAME;
    	this.consumer.subscribe(Arrays.asList(topic));
    	logger.info("消息队列[" + topic + "] 开始初始化...");
	}
}
