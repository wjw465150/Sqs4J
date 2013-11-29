package org.sqs4j.jmx;

public interface Sqs4JMBean {
  boolean flush();

  String status(String httpsqs_input_name);

  String queueNames();
}
