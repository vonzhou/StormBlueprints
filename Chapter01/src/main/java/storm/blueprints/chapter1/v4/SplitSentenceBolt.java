package storm.blueprints.chapter1.v4;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;



public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = -347323985553257367L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			this.collector.emit(tuple, new Values(word));
		}
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
