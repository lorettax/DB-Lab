
package tinydb.common;

/**
 * Debug 是一个实用程序类，它包装 println 语句并允许或多或少地打开命令行输出
 */

public class Debug {
  private static final int DEBUG_LEVEL;
  static {
      String debug = System.getProperty("simpledb.common.Debug");
      if (debug == null) {
          // No system property = disabled
          DEBUG_LEVEL = -1;
      } else if (debug.length() == 0) {
          // Empty property = level 0
          DEBUG_LEVEL = 0;
      } else {
          DEBUG_LEVEL = Integer.parseInt(debug);
      }
  }

  private static final int DEFAULT_LEVEL = 0;

  /** Log message if the log level >= level. Uses printf. */
  public static void log(int level, String message, Object... args) {
    if (isEnabled(level)) {
      System.out.printf(message, args);
      System.out.println();
    }
  }

  /** @return true if level is being logged. */
  public static boolean isEnabled(int level) {
    return level <= DEBUG_LEVEL;
  }

  /** @return true if the default level is being logged. */
  public static boolean isEnabled() {
    return isEnabled(DEFAULT_LEVEL);
  }

  /** Logs message at the default log level. */
  public static void log(String message, Object... args) {
    log(DEFAULT_LEVEL, message, args);
  }
}
