package com.storm.example;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings({"serial","rawtypes"})
public class PrintBolt extends BaseBasicBolt {

    private int indexId;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		this.indexId = context.getThisTaskIndex();
		
    }

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		String rec = tuple.getString(0);
		System.err.println(String.format("Bolt[%d] String recieved: %s",this.indexId, rec));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		// do nothing
		
	}

}
