import org.slf4j.*

class HadoopConf {
  static def inject(Object client, Map storm_conf, String confkey, String filename) {
    def logger = LoggerFactory.getLogger(client.getClass().getName())

    String path = storm_conf[confkey]
    try {
      ResourceOverrideClassLoader.install filename, path
    } catch (IllegalArgumentException e) {
      logger.warn '{} unspecified; {} will be loaded from classpath', confkey, filename
      return
    }
    logger.info "Using {} for {}", path, filename
  }
}
