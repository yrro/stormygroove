import backtype.storm.*
import backtype.storm.testing.*
import backtype.storm.topology.*

class HbaseTopology extends BaseTopology {
  @Override
  TopologyBuilder build() {
    def builder = new TopologyBuilder()
    builder.with {
     setSpout "word", new TestWordSpout(), 2
     setBolt("exclaim1", new ExclamationBolt(), 2).shuffleGrouping "word"
     setBolt("output", new HbaseBolt(), 2).shuffleGrouping "exclaim1"
    }
    return builder
  }

  static void main(String[] args) {
    new HbaseTopology().run(args)
  }
}
