import backtype.storm.task.*
import backtype.storm.topology.*
import backtype.storm.topology.base.*
import backtype.storm.tuple.*

class ExclamationBolt extends BaseRichBolt {
  private OutputCollector o

  @Override
  void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.o = collector
  }

  @Override
  void execute(Tuple tuple) {
    o.emit(tuple, new Values(tuple.getString(0) + "!"))
    o.ack(tuple)
  }

  @Override
  void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("word"))
  }
}


