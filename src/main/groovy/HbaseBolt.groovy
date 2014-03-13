import java.security.PrivilegedExceptionAction
import java.security.PrivilegedActionException

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
class HbaseBolt extends BaseRichBolt {
  private static def logger = LoggerFactory.getLogger(HbaseBolt.class)

  private OutputCollector collector
  String output_table
  UserGroupInformation ugi

  static private injectHadoopConf(Map storm_conf, String confkey, String name) {
    String path = storm_conf[confkey]
    try {
      ResourceOverrideClassLoader.install name, path
      logger.info "Using ${path} for ${name}"
    } catch (IllegalArgumentException e) {
      logger.warn '{} unspecified; {} will be loaded from classpath', confkey, name
    }
  }

  @Override
  void prepare(Map storm_conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector

    injectHadoopConf storm_conf, 'topology.hadoop.conf.core', 'core-site.xml'
    injectHadoopConf storm_conf, 'topology.hbase.conf.hbase', 'hbase-site.xml'

    def user = storm_conf['topology.hadoop.user']
    def keytab = storm_conf['topology.hadoop.keytab']
    if ((user == null) != (keytab == null))
      throw new IllegalArgumentException('missing hdfs user XOR keytab')
    else if (user && keytab)
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab)
    else
      ugi = UserGroupInformation.getCurrentUser()

    output_table = storm_conf['topology.output_table']
    if (!output_table)
      throw new IllegalArgumentException('missing output_table')
  }

  def asUser (Closure cl) {
    try {
      ugi.doAs(cl as PrivilegedExceptionAction)
    } catch (PrivilegedActionException e) {
      throw e.cause
    }
  }

  def write(String message) {
    ugi.reloginFromKeytab()
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
