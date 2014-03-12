import org.slf4j.LoggerFactory

/**
 * This custom classloader injects our hadoop configuration files into all
 * instances of Configuration that Hadoop creates.  Alternative:
 * <https://github.com/nathanmarz/storm/pull/749/files>
 */
class ResourceOverrideClassLoader extends ClassLoader {
  private static def logger = LoggerFactory.getLogger(HdfsBolt.class)

  private def name, url

  ResourceOverrideClassLoader(ClassLoader parent, String name, URL url) {
    super(parent)
    this.name = name
    this.url = url
  }

  @Override
  URL findResource(String name) {
    if (name == this.name) {
      logger.debug 'ResourceOverrideClassLoader found {}', name, new Exception('IT\'S HAPPENING')
    }
    return name == this.name ? this.url : null
  }

  static install(String resource, String file) {
    if (resource == null || file == null)
      throw new IllegalArgumentException("${resource},${file}")

    def loader = new ResourceOverrideClassLoader(
        Thread.currentThread().getContextClassLoader(),
        resource, new File(file).toURI().toURL())
    Thread.currentThread().setContextClassLoader loader
  }
}
