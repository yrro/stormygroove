import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.*
import org.apache.hadoop.security.*

import backtype.storm.task.*
import backtype.storm.topology.*
import backtype.storm.topology.base.*
import backtype.storm.tuple.*

import org.slf4j.LoggerFactory

class HdfsBolt extends BaseAuthBolt {
  private OutputCollector collector
  String output_path

  @Override
  void preparePre(Map storm_conf, TopologyContext context, OutputCollector collector) {
    HadoopConf.inject this, storm_conf, 'topology.hadoop.conf.core', 'core-site.xml'
    HadoopConf.inject this, storm_conf, 'topology.hadoop.conf.hdfs', 'hdfs-site.xml'
  }

  @Override
  void preparePost(Map storm_conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector

    output_path = storm_conf['topology.output_path']
    if (!output_path)
      throw new IllegalArgumentException('missing output_path')
  }

  def write(String message) {
    asUser {
      def hdfs_conf = new Configuration()
      def fs = FileSystem.get(hdfs_conf)
      def os = fs.create(new Path("${output_path}/${UUID.randomUUID()}"), false)
      os << message
      os.close()
    }
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
