import java.security.PrivilegedExceptionAction
import java.security.PrivilegedActionException

import backtype.storm.task.*
import backtype.storm.topology.*
import backtype.storm.topology.base.*

import org.apache.hadoop.security.*

import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class BaseAuthBolt extends BaseRichBolt {
  private UserGroupInformation ugi

  def asUser (Closure cl) {
    ugi.reloginFromKeytab()
    try {
      ugi.doAs(cl as PrivilegedExceptionAction)
    } catch (PrivilegedActionException e) {
      throw e.cause
    }
  }

  abstract void preparePre(Map storm_conf, TopologyContext context, OutputCollector collector)

  abstract void preparePost(Map storm_conf, TopologyContext context, OutputCollector collector)

  final void prepare(Map storm_conf, TopologyContext context, OutputCollector collector) {
    preparePre(storm_conf, context, collector)

    def user = storm_conf['topology.hadoop.user']
    def keytab = storm_conf['topology.hadoop.keytab']
    if ((user == null) != (keytab == null))
      throw new IllegalArgumentException('missing hdfs user XOR keytab')
    else if (user && keytab)
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab)
    else
      ugi = UserGroupInformation.getCurrentUser()

    preparePost(storm_conf, context, collector)
  }
}
