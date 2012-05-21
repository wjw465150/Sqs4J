package org.sqs4j;

import org.tanukisoftware.wrapper.WrapperManager;
import org.tanukisoftware.wrapper.WrapperSimpleApp;

public class WrapperSqs4JApp extends WrapperSimpleApp {
  @Override
  public Integer start(String[] args) {
    Integer result = super.start(args);

    WrapperManager.log(WrapperManager.WRAPPER_LOG_LEVEL_FATAL, "Started Wrapper Sqs4J!");

    return result;
  }

  @Override
  public int stop(int exitCode) {
    int result = super.stop(exitCode);

    WrapperManager.log(WrapperManager.WRAPPER_LOG_LEVEL_FATAL, "Stoped Wrapper Sqs4J!");

    return result;
  }

  protected WrapperSqs4JApp(String[] strings) {
    super(strings);
  }

  public static void main(String args[]) {
    new WrapperSqs4JApp(args);
  }
}
