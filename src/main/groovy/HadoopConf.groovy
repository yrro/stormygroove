import org.slf4j.LoggerFactory

class HadoopConf {
  static def inject(Object client, Map storm_conf, String confkey, String filename) {
    def logger = LoggerFactory.getLogger(client.getClass().getName())

    String path = storm_conf[confkey]
    try {
      ResourceOverrideClassLoader.install filename, path
      logger.info "Using {} for {}", path, filename
    } catch (IllegalArgumentException e) {
      logger.warn '{} unspecified; {} will be loaded from classpath', confkey, filename
    }
  }
}
