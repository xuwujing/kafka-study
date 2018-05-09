package com.pancm.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pancm.constant.Constants;

/**
 * @Title: InsertBolt
 * @Description: 
 * 写入数据的bolt
 * @Version:1.0.0  
 * @author pancm
 * @date 2018年4月19日
 */
public class InsertBolt extends BaseRichBolt{
		private static final long serialVersionUID = 6542256546124282695L;
		
		private static final Logger logger = LoggerFactory.getLogger(InsertBolt.class);
		
		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
		}
		   
		@Override
		public void execute(Tuple tuple) {
			String msg=tuple.getStringByField(Constants.FIELD);
			//业务逻辑从处理...
			logger.info("接受的消息:{}",msg);
		}

		
		/**
	     * cleanup是IBolt接口中定义,用于释放bolt占用的资源。
	     * Storm在终止一个bolt之前会调用这个方法。
		 */
		@Override
		public void cleanup() {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
				
		}
	
}
