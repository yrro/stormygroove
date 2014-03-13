import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.*
import org.apache.hadoop.security.*

import backtype.storm.topology.*
import backtype.storm.topology.base.*
import backtype.storm.task.*
import backtype.storm.tuple.*

import org.slf4j.LoggerFactory

/**
 * Create the HBase table with `hbase shell`:
 *  > create 't1', 'cf1'
 */
class HbaseBolt extends BaseAuthBolt {
  private OutputCollector collector
  String output_table

  @Override
  void prepare(Map storm_conf, TopologyContext context, OutputCollector collector) {
    HadoopConf.inject this, storm_conf, 'topology.hadoop.conf.core', 'core-site.xml'
    HadoopConf.inject this, storm_conf, 'topology.hbase.conf.hbase', 'hbase-site.xml'

    prepareAuth(storm_conf['topology.hadoop.user'], storm_conf['topology.hadoop.keytab'])

    this.collector = collector

    output_table = storm_conf['topology.output_table']
    if (!output_table)
      throw new IllegalArgumentException('missing output_table')
  }

  def write(String message) {
    asUser {
      def hbase_conf = HBaseConfiguration.create()
      def table = new HTable(hbase_conf, output_table)
      def p = new Put(UUID.randomUUID().toString().getBytes('US-ASCII'))
      p.add ('cf1'.getBytes('US-ASCII'), 'word'.getBytes('US-ASCII'), message.getBytes('UTF-8'))
      table.put p
      table.flushCommits()
    }
  }

  @Override
  void execute(Tuple tuple) {
    write(tuple.getString(0))
    collector.ack(tuple)
  }

  @Override
  void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("word"))
  }
}
