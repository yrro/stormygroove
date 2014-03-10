import backtype.storm.Config
import backtype.storm.testing.*
import backtype.storm.topology.*

class Topology {
  static def conf(String[] args) {
    def conf = new Config()
    conf.setDebug(true)
    conf['topology.output_path'] = args[0]
    return conf
  }
  static def build() {
    def builder = new TopologyBuilder()
    builder.with {
     setSpout "word", new TestWordSpout(), 10
     setBolt("exclaim1", new ExclaimationBolt(), 3).shuffleGrouping "word"
     setBolt("output", new HdfsBolt(), 2).shuffleGrouping "exclaim1"
    }
    return builder
  }
}
