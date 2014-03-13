import backtype.storm.*
import backtype.storm.testing.*
import backtype.storm.topology.*

class HdfsTopology extends BaseTopology {
  @Override
  Config configure(List args) {
    if (args.size != 1) {
      return null
    }

    def conf = new Config()
    conf['topology.output_path'] = args[0]
    conf['topology.hadoop.conf.core'] = '/etc/hadoop/conf/core-site.xml'
    conf['topology.hadoop.conf.hdfs'] = '/etc/hadoop/conf/hdfs-site.xml'
    conf['topology.hadoop.user'] = 'storm'
    conf['topology.hadoop.keytab'] = '/tmp/storm.keytab'
    return conf
  }

  @Override
  TopologyBuilder build() {
    def builder = new TopologyBuilder()
    builder.with {
     setSpout "word", new TestWordSpout(), 2
     setBolt("exclaim1", new ExclamationBolt(), 2).shuffleGrouping "word"
     setBolt("output", new HdfsBolt(), 2).shuffleGrouping "exclaim1"
    }
    return builder
  }

  static void main(String[] args) {
    new HdfsTopology().run(args)
  }
}
