import backtype.storm.*

class SubmitTopology {
  static void main(String[] args) {
    def conf = new Config()
    conf.setDebug(true)

    conf.setNumWorkers(3)
    StormSubmitter.submitTopology("foo", conf, Topology.build().createTopology())
  }
}
