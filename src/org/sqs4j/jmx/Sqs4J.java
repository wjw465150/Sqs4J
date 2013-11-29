package org.sqs4j.jmx;

import java.util.List;

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
      StringBuilder buf = new StringBuilder();
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

      buf.append(String.format("{ \"name\": \"%s\",\r\n  \"maxqueue\": %d,\r\n  \"putpos\": %d,\r\n  \"putlap\": \"%s\",\r\n  \"getpos\": %d,\r\n  \"getlap\": \"%s\",\r\n  \"unread\": %d,\r\n  \"sync\": \"%s\"\r\n}\n", httpsqs_input_name, maxqueue, putpos, put_times, getpos, get_times, ungetnum, !_app._scheduleSync.isShutdown()));

      return buf.toString();
      //return _app._db.getProperty("leveldb.stats");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      return e.getMessage();
    }
  }

  @Override
  public String queueNames() {
    List<String> lst = _app.getQueueNames();

    StringBuilder buf = new StringBuilder("{ \"size\": " + lst.size() + ",\r\n  \"queues\": [\r\n");
    for (int i = 0; i < lst.size(); i++) {
      buf.append("    \"" + lst.get(i) + "\"");
      if (i != (lst.size() - 1)) {
        buf.append(",\r\n");
      }
    }
    buf.append("\r\n  ]" + "\r\n}\r\n");

    return buf.toString();

  }
}
