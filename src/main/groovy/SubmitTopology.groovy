import backtype.storm.*

class SubmitTopology {
  static void main(String[] args) {
    def conf = Topology.conf(args)
    conf.setNumWorkers(3)
    StormSubmitter.submitTopology("foo", conf, Topology.build().createTopology())
  }
}
