import backtype.storm.*
import backtype.storm.utils.*

class TestTopology {
  static void main(String[] args) {
    def conf = new Config()
    conf.setDebug(true)

    def cluster = new LocalCluster()
    cluster.submitTopology("foo", conf, Topology.build().createTopology())
    Utils.sleep(5000)
    cluster.killTopology("foo")
    cluster.shutdown()
  }
}
