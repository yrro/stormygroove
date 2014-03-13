import backtype.storm.*
import backtype.storm.topology.*
import backtype.storm.utils.*

abstract class BaseTopology {
  abstract Config configure(List args);

  abstract TopologyBuilder build();

  void run(String[] args) {
    def cli = new CliBuilder(usage:"storm jar stormygroove.jar ${getClass().getName()} [OPTIONS] [TOPOLOGY ARGS...]")
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

    def name = opts.n ? opts.n : getClass().getName();

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
