package org.sqs4j.jmx;

import org.sqs4j.Sqs4jApp;

public class Sqs4J implements Sqs4JMBean{
  private Sqs4jApp _app;

  public Sqs4J(Sqs4jApp app) {
    _app = app;
  }
  
  @Override
  public boolean flush() {
    _app.flush();
    return true;
  }

}
