import backtype.storm.*
import backtype.storm.utils.*

class TestTopology {
  static void main(String[] args) {
    def conf = Topology.conf(args)
    def cluster = new LocalCluster()
    cluster.submitTopology("foo", conf, Topology.build().createTopology())
    Utils.sleep(5000)
    cluster.killTopology("foo")
    cluster.shutdown()
  }
}
