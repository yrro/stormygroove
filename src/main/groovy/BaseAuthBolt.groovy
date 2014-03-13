import java.security.PrivilegedExceptionAction
import java.security.PrivilegedActionException

import backtype.storm.topology.base.*

import org.apache.hadoop.security.*

import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class BaseAuthBolt extends BaseRichBolt {
  protected Logger logger

  private UserGroupInformation ugi

  BaseAuthBolt() {
    logger = LoggerFactory.getLogger(getClass().getName())
  }

  def asUser (Closure cl) {
    ugi.reloginFromKeytab()
    try {
      ugi.doAs(cl as PrivilegedExceptionAction)
    } catch (PrivilegedActionException e) {
      throw e.cause
    }
  }

  def prepareAuth(String user, String keytab) {
    if ((user == null) != (keytab == null))
      throw new IllegalArgumentException('missing hdfs user XOR keytab')
    else if (user && keytab)
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab)
    else
      ugi = UserGroupInformation.getCurrentUser()
  }
}
