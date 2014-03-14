import backtype.storm.*
import backtype.storm.testing.*
import backtype.storm.topology.*

class HdfsTopology extends BaseTopology {
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
