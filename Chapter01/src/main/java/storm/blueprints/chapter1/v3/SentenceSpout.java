package storm.blueprints.chapter1.v3;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.blueprints.utils.Utils;


public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -5377168602207925727L;

	private SpoutOutputCollector collector;
	private final String[] sentences = { "my dog has fleas", "i like cold beverages", "the dog ate my homework",
			"don't have a cow man", "i don't think i like fleas" };
	private int index = 0;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (index < sentences.length) {
			this.collector.emit(new Values(sentences[index]));
			index++;
		}
		Utils.waitForMillis(1);
	}

}
