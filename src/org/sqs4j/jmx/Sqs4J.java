package org.sqs4j.jmx;

import org.sqs4j.Sqs4jApp;

public class Sqs4J implements Sqs4JMBean {
  private Sqs4jApp _app;

  public Sqs4J(Sqs4jApp app) {
    _app = app;
  }

  @Override
  public boolean flush() {
    _app.flush();
    return true;
  }

  @Override
  public String status(String httpsqs_input_name) {
    try {
    StringBuilder _buf = new StringBuilder();
    String put_times = "1st lap";
    final String get_times = "1st lap";

    final long maxqueue = _app.httpsqs_read_maxqueue(httpsqs_input_name); /* 最大队列数量 */
    final long putpos = _app.httpsqs_read_putpos(httpsqs_input_name); /* 入队列写入位置 */
    final long getpos = _app.httpsqs_read_getpos(httpsqs_input_name); /* 出队列读取位置 */
    long ungetnum = 0;
    if (putpos >= getpos) {
      ungetnum = Math.abs(putpos - getpos); //尚未出队列条数
    } else if (putpos < getpos) {
      ungetnum = Math.abs(maxqueue - getpos + putpos); /* 尚未出队列条数 */
      put_times = "2nd lap";
    }
    _buf.append(String.format("HTTP Simple Queue Service (Sqs4J)v%s\n", Sqs4jApp.VERSION));
    _buf.append("------------------------------\n");
    _buf.append(String.format("Queue Name: %s\n", httpsqs_input_name));
    _buf.append(String.format("Maximum number of queues: %d\n", maxqueue));
    _buf.append(String.format("Put position of queue (%s): %d\n", put_times, putpos));
    _buf.append(String.format("Get position of queue (%s): %d\n", get_times, getpos));
    _buf.append(String.format("Number of unread queue: %d\n", ungetnum));
    _buf.append("ScheduleSync running: " + !_app._scheduleSync.isShutdown());

    return _buf.toString();
    //return _app._db.getProperty("leveldb.stats");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      return e.getMessage();
    }
  }
}
