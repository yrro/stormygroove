import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*

import backtype.storm.topology.*
import backtype.storm.topology.base.*
import backtype.storm.task.*
import backtype.storm.tuple.*

import org.slf4j.LoggerFactory

class HdfsBolt extends BaseRichBolt {
  private static def logger = LoggerFactory.getLogger(HdfsBolt.class)

  private FileSystem fs
  private OutputCollector collector
  String output_path

  static private def injectHadoopConf(Map storm_conf, String confkey, String name) {
    String path = storm_conf[confkey]
    try {
      ResourceOverrideClassLoader.install name, path
      logger.info "Using ${path} for ${name}"
    } catch (IllegalArgumentException e) {
      logger.warn '{} unspecified; {} will be loaded from classpath', confkey, name
    }
  }

  @Override
  void prepare(Map storm_conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector

    injectHadoopConf storm_conf, 'topology.hadoop.conf.core', 'core-site.xml'
    injectHadoopConf storm_conf, 'topology.hadoop.conf.hdfs', 'hdfs-site.xml'

    def hdfs_conf = new Configuration()
    fs = FileSystem.get(hdfs_conf)

    output_path = storm_conf['topology.output_path']
    if (!output_path)
      throw new IllegalArgumentException('missing output_path')
  }

  def write(String message) {
    def os = fs.create(new Path("${output_path}/${UUID.randomUUID()}"), false)
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
