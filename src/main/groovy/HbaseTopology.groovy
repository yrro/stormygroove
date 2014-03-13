import backtype.storm.*
import backtype.storm.utils.*
import backtype.storm.testing.*
import backtype.storm.topology.*

class HbaseTopology {
  static configure(List args) {
    if (args.size != 1) {
      return null
    }

    def conf = new Config()
    conf['topology.output_table'] = args[0]
    conf['topology.hadoop.conf.core'] = '/etc/hadoop/conf/core-site.xml'
    conf['topology.hbase.conf.hbase'] = '/etc/hbase/conf/hbase-site.xml'
    conf['topology.hadoop.user'] = 'storm'
    conf['topology.hadoop.keytab'] = '/tmp/storm.keytab'
    return conf
  }

  static def build() {
    def builder = new TopologyBuilder()
    builder.with {
     setSpout "word", new TestWordSpout(), 2
     setBolt("exclaim1", new ExclaimationBolt(), 2).shuffleGrouping "word"
     setBolt("output", new HbaseBolt(), 2).shuffleGrouping "exclaim1"
    }
    return builder
  }

  static void main(String[] args) {
    def cli = new CliBuilder(usage:'storm jar stormygroove.jar HbaseTopology OUTPUT_TABLE')
    cli.d('storm debug')
    cli.h('show help and exit')
    cli.l('run with LocalCluster', args:1, argName:'timeout')
    cli.n('topology name', args:1, argName:'name')
    cli.w('storm worker count', args:1, argName:'count')
    def opts = cli.parse(args)
    if (opts.h) {
      cli.usage()
      System.exit 0
    }

    def name = opts.n ? opts.n : 'HbaseTopology'

    def conf = configure(opts.arguments())
    if (conf == null) {
      cli.usage()
      System.exit 1
    }
    if (opts.d)
      conf.setDebug(true)
    if (opts.w)
      conf.setNumWorkers(opts.w.toInteger())

    def topology = build().createTopology()

    if (opts.l) {
      def cluster = new LocalCluster()
      cluster.submitTopology(name, conf, topology)
      Utils.sleep(1000L * opts.l.toInteger())
      cluster.killTopology(name)
      cluster.shutdown()
    } else {
      StormSubmitter.submitTopology(name, conf, topology)
    }
  }
}
