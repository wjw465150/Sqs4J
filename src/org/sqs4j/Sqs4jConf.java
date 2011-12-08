package org.sqs4j;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.core.Persister;
import org.simpleframework.xml.stream.Format;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Sqs4J配置文件 User: wstone Date: 2010-7-30 Time: 13:08:46
 */
@Root(name = "Sqs4j")
public class Sqs4jConf {
  @Element
  public String bindAddress = "*"; //监听地址,*代表所有

  @Element
  public int bindPort = 1218; //监听端口

  @Element
  public int backlog = 200; //侦听 backlog 长度

  @Element
  public int soTimeout = 60; //HTTP请求的超时时间(秒)

  @Element
  public String defaultCharset = "UTF-8"; //缺省字符集
  public Charset charsetDefaultCharset = Charset.forName(defaultCharset); //HTTP字符集

  @Element(required = false)
  public String dbPath = ""; //数据库目录,缺省在:System.getProperty("user.dir", ".") + "/db"

  @Element
  public int syncinterval = 1; //同步更新内容到磁盘的间隔时间

  @Element
  public String adminUser = "admin"; //管理员用户名

  @Element
  public String adminPass = "123456"; //管理员口令

  @Element
  public int jmxPort = 1219; //JMX监听端口

  @Element(required = false)
  public String auth = ""; //Sqs4j的get,put,view的验证密码,为空时不验证

  public static Sqs4jConf load(String path) throws Exception {
    Persister serializer = new Persister();

    InputStreamReader reader = null;
    try {
      reader = new InputStreamReader(new FileInputStream(path), "UTF-8");
      Sqs4jConf conf = serializer.read(Sqs4jConf.class, reader);
      return conf;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
        }
      }
    }
  }

  public void store(String path) throws Exception {
    Persister serializer = new Persister(new Format(2, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    OutputStreamWriter writer = null;
    try {
      if (dbPath.equals(System.getProperty("user.dir", ".") + "/db")) {
        dbPath = "";
      }
      writer = new OutputStreamWriter(new FileOutputStream(path), "UTF-8");
      serializer.write(this, writer);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ex) {
        }
      }
    }
  }

  @Override
  public String toString() {
    return "Sqs4jConf{" + "bindAddress='" + bindAddress + '\'' + ", bindPort=" + bindPort + ", backlog=" + backlog
        + ", soTimeout=" + soTimeout + ", defaultCharset='" + defaultCharset + '\'' + ", dbPath='" + dbPath + '\''
        + ", syncinterval=" + syncinterval + ", adminUser='" + adminUser + '\'' + ", adminPass='" + adminPass + '\''
        + ", jmxPort='" + jmxPort + '\'' + ", auth='" + auth + '\'' + '}';
  }
}
