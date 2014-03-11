import backtype.storm.Config
import backtype.storm.testing.*
import backtype.storm.topology.*

class Topology {
  static def conf(String[] args) {
    def conf = new Config()
    //conf.setDebug(true)
    //conf['topology.worker.classpath'] = '/etc/hadoop/conf'
    conf['topology.output_path'] = args[0]
    conf['topology.hadoop.conf.core'] = '/etc/hadoop/conf/core-site.xml'
    conf['topology.hadoop.conf.hdfs'] = '/etc/hadoop/conf/hdfs-site.xml'
    //conf['topology.hadoop.conf.mapred'] = '/etc/hadoop/conf/mapred-site.xml'
    //conf['topology.hadoop.conf.yarn'] = '/etc/hadoop/conf/yarn-site.xml'
    //conf['topology.hbase.conf.hbase'] = '/etc/hbase/conf/hbase-site.xml'
    return conf
  }
  static def build() {
    def builder = new TopologyBuilder()
    builder.with {
     setSpout "word", new TestWordSpout(), 2
     setBolt("exclaim1", new ExclaimationBolt(), 2).shuffleGrouping "word"
     setBolt("output", new HdfsBolt(), 2).shuffleGrouping "exclaim1"
    }
    return builder
  }
}
