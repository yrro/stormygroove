import backtype.storm.*
import backtype.storm.topology.*
import backtype.storm.utils.*

abstract class BaseTopology {
  abstract TopologyBuilder build()

  void run(String[] args) {
    def cli = new CliBuilder(usage:"storm jar stormygroove.jar ${getClass().getName()} [OPTIONS] [TOPOLOGY ARGS...]")
    cli.with {
      h('show help and exit')
      n('topology name', args:1, argName:'name')
      l('run with LocalCluster', args:1, argName:'timeout')
      P('load properties from file', args:1, argName:'file')
      p('set individual property', args:2, valueSeparator:'=', argName:'property=value')
      d('storm debug')
      w('storm worker count', args:1, argName:'count')
    }
    def opts = cli.parse(args)
    if (opts.h) {
      cli.usage()
      System.exit 0
    }
    else if (!opts.arguments().empty) {
      cli.usage()
      System.exit 1
    }

    def conf = new Config()
    opts.Ps.each {
      new File(it).withInputStream {
        def props = new Properties()
        props.load it
        conf.putAll(props)
      }
    }
    opts.ps.collate(2, false).each {k, v -> conf[k]=v}
    if (opts.d)
      conf.setDebug(true)
    if (opts.w)
      conf.setNumWorkers(opts.w.toInteger())

    def name = opts.n ? opts.n : getClass().getName();

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
