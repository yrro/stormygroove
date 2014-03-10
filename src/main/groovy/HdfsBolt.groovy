import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.*

import backtype.storm.topology.*
import backtype.storm.topology.base.*
import backtype.storm.task.*
import backtype.storm.tuple.*

class HdfsBolt extends BaseRichBolt {
  private FileSystem fs
  private OutputCollector collector
  String output_path

  @Override
  void prepare(Map storm_conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector

    def hdfs_conf = new Configuration()
    fs = FileSystem.get(hdfs_conf)

    output_path = storm_conf['topology.output_path']
    if (!output_path)
      throw new Exception('missing output_path')
  }

  def write(String message) {
    def os = fs.create(new Path("${output_path}/${UUID.randomUUID()}"))
    os << message
    os.close()
  }

  @Override
  void execute(Tuple tuple) {
    write(tuple.getString(0))
    collector.ack(tuple)
  }

  @Override
  void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("word"))
  }
}


