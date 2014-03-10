import backtype.storm.testing.*
import backtype.storm.topology.*

class Topology {
  static def build() {
    def builder = new TopologyBuilder()
    builder.with {
     setSpout "word", new TestWordSpout(), 10
     setBolt("exclaim1", new ExclaimationBolt(), 3).shuffleGrouping "word"
     setBolt("exclaim2", new ExclaimationBolt(), 2).shuffleGrouping "exclaim1" 
    }
    return builder
  }
}
