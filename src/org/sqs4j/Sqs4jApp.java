package org.sqs4j;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXPrincipal;
import javax.management.remote.JMXServiceURL;
import javax.security.auth.Subject;

import net.kotek.jdbm.DB;
import net.kotek.jdbm.DBMaker;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * 基于HTTP协议的轻量级开源简单队列服务. User: wstone Date: 2010-7-30 Time: 11:44:52
 */
public class Sqs4jApp implements Runnable {
  static final String VERSION = "1.3.8"; //当前版本
  static final String DB_CHARSET = "UTF-8"; //数据库字符集
  static final long DEFAULT_MAXQUEUE = 1000000000; //缺省队列最大数是10亿条
  private final String CONF_NAME; //配置文件

  private org.slf4j.Logger _log = org.slf4j.LoggerFactory.getLogger(this.getClass());
  Sqs4jConf _conf; //配置文件

  private boolean _rmiCreated;
  private Registry _rmiRegistry; //RIM 注册表
  private JMXConnectorServer _jmxCS; //JMXConnectorServer

  static Lock _lock = new ReentrantLock(); //HTTP请求并发锁
  public DB _db; //数据库

  //同步磁盘的Scheduled
  ScheduledExecutorService _scheduleSync = Executors.newSingleThreadScheduledExecutor();

  private Channel _channel; //Socket通道

  //初始化目录和Log4j
  static {
    try {
      File file = new File(System.getProperty("user.dir", ".") + "/conf/");
      if (!file.exists() && !file.mkdirs()) {
        throw new IOException("Can not create:" + System.getProperty("user.dir", ".") + "/conf/");
      }

      file = new File(System.getProperty("user.dir", ".") + "/db/");
      if (!file.exists() && !file.mkdirs()) {
        throw new IOException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
      }

      String logPath = System.getProperty("user.dir", ".") + "/conf/log4j.xml";
      if (logPath.toLowerCase().endsWith(".xml")) {
        DOMConfigurator.configure(logPath);
      } else {
        PropertyConfigurator.configure(logPath);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void main(String args[]) {
    @SuppressWarnings("unused")
    Sqs4jApp app = new Sqs4jApp(args);
  }

  @Override
  //定时将内存中的内容写入磁盘
  public void run() {
    this.flush();
  }

  public void flush() {
    try {
      _db.commit();
      _db.sync();
    } catch (Throwable thex) {
      thex.printStackTrace();
    }
  }
  
  /**
   * 构造函数
   * 
   * @param args
   */
  public Sqs4jApp(String args[]) {
    java.lang.Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        doStop2();
      }
    }));

    CONF_NAME = System.getProperty("user.dir", ".") + "/conf/Sqs4jConf.xml";

    if (!this.doStart2()) {
      System.exit(-1);
    }

    //    for (; ;) {
    //      try {
    //        java.util.concurrent.TimeUnit.SECONDS.sleep(10);
    //      } catch (InterruptedException ex) {
    //        Thread.currentThread().interrupt();
    //        System.exit(0);
    //      }
    //    }

  }

  String getBASE64DecodeOfStr(String inStr, String charset) throws UnsupportedEncodingException, IOException {
    return new String(Base64.decode(inStr), charset);
  }

  /**
   * 从HTTP Header里找到字符集编码,没有发现返回null
   * 
   * @param contentType
   * @return
   */
  String getCharsetFromContentType(String contentType) {
    if (contentType == null) {
      return null;
    }
    int start = contentType.indexOf("charset=");
    if (start < 0) {
      return null;
    }
    String encoding = contentType.substring(start + 8);
    int end = encoding.indexOf(';');
    if (end >= 0) {
      encoding = encoding.substring(0, end);
    }
    encoding = encoding.trim();
    if ((encoding.length() > 2) && (encoding.startsWith("\"")) && (encoding.endsWith("\""))) {
      encoding = encoding.substring(1, encoding.length() - 1);
    }
    return (encoding.trim());
  }

  /**
   * 从HTTP的URL的请求参数里找到字符集编码,没有发现返回null
   * 
   * @param query
   * @return
   */
  String getCharsetFromQuery(String query) {
    if (query == null) {
      return null;
    }
    int start = query.indexOf("charset=");
    if (start < 0) {
      return null;
    }
    String encoding = query.substring(start + 8);
    int end = encoding.indexOf('&');
    if (end >= 0) {
      encoding = encoding.substring(0, end);
    }
    encoding = encoding.trim();
    return encoding;
  }

  /**
   * 把HTTP的URL的请求参数解析成Map
   * 
   * @param query
   * @param charset
   * @return
   */
  private Map<String, String> makeParameters(String query, String charset) {
    Map<String, String> map = new HashMap<String, String>();
    if (query == null || charset == null) {
      return map;
    }

    String[] keyValues;
    keyValues = query.split("&");
    for (String keyValue : keyValues) {
      String[] kv = keyValue.split("=");
      if (kv.length == 2) {
        try {
          map.put(kv[0], URLDecoder.decode(kv[1], charset));
        } catch (UnsupportedEncodingException e) {
          //
        }
      }
    }

    return map;
  }

  Map<String, String> getHashMap(String httpsqs_input_name) {
    Map<String, String> map = _db.getHashMap(httpsqs_input_name);
    if (map == null) {
      map = _db.createHashMap(httpsqs_input_name);
    }

    return map;
  }

  /* 读取队列写入点的值 */

  long httpsqs_read_putpos(String httpsqs_input_name) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);

    String key = "putpos";
    String value = map.get(key);
    if (value == null) {
      return 0;
    } else {
      return Long.parseLong(value);
    }
  }

  /* 读取队列读取点的值 */

  long httpsqs_read_getpos(String httpsqs_input_name) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);

    String key = "getpos";
    String value = map.get(key);
    if (value == null) {
      return 0;
    } else {
      return Long.parseLong(value);
    }
  }

  /* 读取用于设置的最大队列数 */

  long httpsqs_read_maxqueue(String httpsqs_input_name) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);

    String key = "maxqueue";
    String value = map.get(key);
    if (value == null) {
      return DEFAULT_MAXQUEUE;
    } else {
      return Long.parseLong(value);
    }
  }

  /**
   * 设置最大的队列数量，返回值为设置的队列数量。如果返回值为0，则表示设置取消（取消原因为：
   * 设置的最大的队列数量小于”当前队列写入位置点“和”当前队列读取位置点“，或者”当前队列写入位置点“小于”当前队列的读取位置点）
   * 
   * @param httpsqs_input_name
   * @param httpsqs_input_num
   * @return
   */
  long httpsqs_maxqueue(String httpsqs_input_name, long httpsqs_input_num) throws UnsupportedEncodingException {
    long queue_put_value = httpsqs_read_putpos(httpsqs_input_name);
    long queue_get_value = httpsqs_read_getpos(httpsqs_input_name);

    /* 设置的最大的队列数量必须大于等于”当前队列写入位置点“和”当前队列读取位置点“，并且”当前队列写入位置点“必须大于等于”当前队列读取位置点“ */
    if (httpsqs_input_num >= queue_put_value && httpsqs_input_num >= queue_get_value
        && queue_put_value >= queue_get_value) {
      Map<String, String> map = getHashMap(httpsqs_input_name);
      String key = "maxqueue";
      map.put(key, String.valueOf(httpsqs_input_num));

      this.flush(); //实时刷新到磁盘
      _log.info(String.format("队列配置被修改:(%s:maxqueue)=%d", httpsqs_input_name, httpsqs_input_num));
      return httpsqs_input_num;
    } else {
      return 0L;
    }
  }

  /**
   * 重置队列，true表示重置成功
   * 
   * @param httpsqs_input_name
   * @return
   */
  boolean httpsqs_reset(String httpsqs_input_name) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);
    String key = "putpos";
    map.remove(key);

    key = "getpos";
    map.remove(key);

    key = "maxqueue";
    map.remove(key);

    this.flush(); //实时刷新到磁盘

    return true;
  }

  /**
   * 查看单条队列内容
   * 
   * @param httpsqs_input_name
   * @param pos
   * @return
   */
  String httpsqs_view(String httpsqs_input_name, long pos) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);

    String key = String.valueOf(pos);
    String value = map.get(key);

    return value;
  }

  /**
   * 修改定时更新内存内容到磁盘的间隔时间，返回间隔时间（秒）
   * 
   * @param httpsqs_input_num
   * @return
   */
  int httpsqs_synctime(int httpsqs_input_num) {
    if (httpsqs_input_num >= 1) {
      _conf.syncinterval = httpsqs_input_num;
      try {
        _conf.store(CONF_NAME);
        _scheduleSync.shutdown();

        _scheduleSync = Executors.newSingleThreadScheduledExecutor();
        _scheduleSync.scheduleWithFixedDelay(this, 1, _conf.syncinterval, TimeUnit.SECONDS);
        _log.info("配置文件被修改:" + _conf.toString());
      } catch (Exception ex) {
        _log.error(ex.getMessage(), ex);
      }
    }

    return _conf.syncinterval;
  }

  /**
   * 获取本次“入队列”操作的队列写入点
   * 
   * @param httpsqs_input_name
   * @return
   */
  long httpsqs_now_putpos(String httpsqs_input_name) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);

    long maxqueue_num = httpsqs_read_maxqueue(httpsqs_input_name);
    long queue_put_value = httpsqs_read_putpos(httpsqs_input_name);
    long queue_get_value = httpsqs_read_getpos(httpsqs_input_name);

    String key = "putpos";
    /* 队列写入位置点加1 */
    queue_put_value = queue_put_value + 1;
    if (queue_put_value > maxqueue_num && queue_get_value == 0) { /*
                                                                   * 如果队列写入ID+1
                                                                   * 之后追上队列读取ID
                                                                   * ，则说明队列已满
                                                                   * ，返回0，拒绝继续写入
                                                                   */
      queue_put_value = 0;
    } else if (queue_put_value == queue_get_value) { /*
                                                      * 如果队列写入ID+1之后追上队列读取ID，则说明队列已满
                                                      * ，返回0，拒绝继续写入
                                                      */
      queue_put_value = 0;
    } else if (queue_put_value > maxqueue_num) { /*
                                                  * 如果队列写入ID大于最大队列数量，则重置队列写入位置点的值为1
                                                  */
      String value = "1";
      map.put(key, value);
    } else { /* 队列写入位置点加1后的值，回写入数据库 */
      String value = String.valueOf(queue_put_value);
      map.put(key, value);
    }

    return queue_put_value;
  }

  /**
   * 获取本次“出队列”操作的队列读取点，返回值为0时队列全部读取完成
   * 
   * @param httpsqs_input_name
   * @return
   */
  long httpsqs_now_getpos(String httpsqs_input_name) throws UnsupportedEncodingException {
    Map<String, String> map = getHashMap(httpsqs_input_name);

    long maxqueue_num = httpsqs_read_maxqueue(httpsqs_input_name);
    long queue_put_value = httpsqs_read_putpos(httpsqs_input_name);
    long queue_get_value = httpsqs_read_getpos(httpsqs_input_name);

    String key = "getpos";
    /* 如果queue_get_value的值不存在，重置为1 */
    if (queue_get_value == 0 && queue_put_value > 0) {
      queue_get_value = 1;
      String value = "1";
      map.put(key, value);

      /* 如果队列的读取值（出队列）小于队列的写入值（入队列） */
    } else if (queue_get_value < queue_put_value) {
      queue_get_value = queue_get_value + 1;
      String value = String.valueOf(queue_get_value);
      map.put(key, value);

      /* 如果队列的读取值（出队列）大于队列的写入值（入队列），并且队列的读取值（出队列）小于最大队列数量 */
    } else if (queue_get_value > queue_put_value && queue_get_value < maxqueue_num) {
      queue_get_value = queue_get_value + 1;
      String value = String.valueOf(queue_get_value);
      map.put(key, value);

      /* 如果队列的读取值（出队列）大于队列的写入值（入队列），并且队列的读取值（出队列）等于最大队列数量 */
    } else if (queue_get_value > queue_put_value && queue_get_value == maxqueue_num) {
      queue_get_value = 1;
      String value = "1";
      map.put(key, value);

      /* 队列的读取值（出队列）等于队列的写入值（入队列），即队列中的数据已全部读出 */
    } else {
      queue_get_value = 0;
    }

    return queue_get_value;
  }

  public boolean doStart2() {
    try {
      try {
        _conf = Sqs4jConf.load(CONF_NAME);
      } catch (Exception ex) {
        //ex.printStackTrace();
        _conf = new Sqs4jConf();
        _conf.store(CONF_NAME);
      }
      if (_conf.dbPath == null || _conf.dbPath.length() == 0) {
        _conf.dbPath = System.getProperty("user.dir", ".") + "/db";
      }
      if (_conf.auth != null && _conf.auth.trim().length() == 0) {
        _conf.auth = null;
      }

      if (_jmxCS == null) {
        Map<String, Object> env = new HashMap<String, Object>();
        env.put(JMXConnectorServer.AUTHENTICATOR, new JMXAuthenticator() {
          public Subject authenticate(Object credentials) {
            String[] sCredentials = (String[]) credentials;
            String userName = sCredentials[0];
            String password = sCredentials[1];
            if (_conf.adminUser.equals(userName) && _conf.adminPass.equals(password)) {
              Set<Principal> principals = new HashSet<Principal>();
              principals.add(new JMXPrincipal(userName));
              return new Subject(true, principals, Collections.EMPTY_SET, Collections.EMPTY_SET);
            }

            throw new SecurityException("Authentication failed! ");
          }
        });

        synchronized (LocateRegistry.class) {
          try {
            _rmiRegistry = LocateRegistry.getRegistry(_conf.jmxPort);
            _rmiRegistry.list();
            _rmiCreated = false;
            _log.info("Detect RMI registry:" + _rmiRegistry.toString());
          } catch (RemoteException ex) {
            _rmiRegistry = LocateRegistry.createRegistry(_conf.jmxPort);
            _rmiRegistry.list();
            _rmiCreated = true;
            _log.info("Could not detect local RMI registry - creating new one:" + _rmiRegistry.toString());
          }
        }

        String serviceUrl = "service:jmx:rmi://0.0.0.0:" + _conf.jmxPort + "/jndi/rmi://127.0.0.1:" + _conf.jmxPort
            + "/jmxrmi";
        _jmxCS = JMXConnectorServerFactory.newJMXConnectorServer(new JMXServiceURL(serviceUrl), env, java.lang.management.ManagementFactory.getPlatformMBeanServer());
        _jmxCS.start();
        registerMBean(new org.sqs4j.jmx.Sqs4J(this), " org.sqs4j:type=Sqs4J");
      }

      if (_db == null) {
        if (_conf.dbPath == null || _conf.dbPath.length() == 0) {
          _conf.dbPath = System.getProperty("user.dir", ".") + "/db";
        }
        //int cacheSize = 100000;
        //System.out.println("MRUCacheSize:" + cacheSize);
        _db = new DBMaker(_conf.dbPath + "/sqs4j.db")
            .closeOnExit()
            .disableAutoDefrag()
            .disableTransactions()
            //.setMRUCacheSize(cacheSize)  @wjw_note: 搞不懂,设置缓存还会降低性能.
            .build();
      }

      _scheduleSync.scheduleWithFixedDelay(this, 1, _conf.syncinterval, TimeUnit.SECONDS);

      if (_channel == null) {
        InetSocketAddress addr;
        if (_conf.bindAddress.equals("*")) {
          addr = new InetSocketAddress(_conf.bindPort);
        } else {
          addr = new InetSocketAddress(_conf.bindAddress, _conf.bindPort);
        }

        ServerBootstrap _server = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        _server.setOption("tcpNoDelay", true);
        _server.setOption("reuseAddress", true);
        _server.setOption("soTimeout", _conf.soTimeout * 1000);
        _server.setOption("backlog", _conf.backlog);

        _server.setPipelineFactory(new HttpServerPipelineFactory(this));
        _channel = _server.bind(addr);

        _log.info(String.format("Sqs4J Server is listening on Address:%s Port:%d\n%s", _conf.bindAddress, _conf.bindPort, _conf.toString()));
      }
      System.out.println("Started Sqs4J!");

      return true;
    } catch (Throwable ex) {
      _log.error(ex.getMessage(), ex);
      return false;
    }
  }

  public boolean doStop2() {
    _scheduleSync.shutdown();

    if (_channel != null) {
      try {
        _log.info("Now stoping Sqs4J Server ......");
        ChannelFuture channelFuture = _channel.close();
        channelFuture.awaitUninterruptibly();
      } catch (Throwable ex) {
        _log.error(ex.getMessage(), ex);
      } finally {
        _channel = null;
        _log.info("Sqs4J Server is stoped!");
      }

    }

    if (_db != null) {
      this.flush(); //实时刷新到磁盘

      try {
        _db.close();
      } catch (Throwable ex) {
        _log.error(ex.getMessage(), ex);
      } finally {
        _db = null;
      }
    }

    if (_jmxCS != null) {
      try {
        _jmxCS.stop();
      } catch (Throwable ex) {
        _log.error(ex.getMessage(), ex);
      } finally {
        _jmxCS = null;
      }
    }

    if (_rmiCreated && _rmiRegistry != null) {
      try {
        UnicastRemoteObject.unexportObject(_rmiRegistry, true);
      } catch (Throwable ex) {
        _log.error(ex.getMessage(), ex);
      } finally {
        _rmiRegistry = null;
      }
    }

    System.out.println("Stoped Sqs4J!");
    return true;
  }

  /**
   * Java 1.5 and above supports the ability to register the WrapperManager
   * MBean internally.
   */
  @SuppressWarnings("rawtypes")
  private void registerMBean(Object mbean, String name) {
    Class classManagementFactory;
    Class classMBeanServer;
    Class classObjectName;
    try {
      classManagementFactory = Class.forName("java.lang.management.ManagementFactory");
      classMBeanServer = Class.forName("javax.management.MBeanServer");
      classObjectName = Class.forName("javax.management.ObjectName");
    } catch (ClassNotFoundException e) {
      _log.error("Registering MBeans not supported by current JVM:" + name);
      return;
    }

    try {
      // This code uses reflection so it combiles on older JVMs.
      // The original code is as follows:
      // javax.management.MBeanServer mbs =
      //     java.lang.management.ManagementFactory.getPlatformMBeanServer();
      // javax.management.ObjectName oName = new javax.management.ObjectName( name );
      // mbs.registerMBean( mbean, oName );

      // The version of the above code using reflection follows.
      Method methodGetPlatformMBeanServer = classManagementFactory.getMethod("getPlatformMBeanServer", (Class[]) null);
      Constructor constructorObjectName = classObjectName.getConstructor(new Class[] { String.class });
      Method methodRegisterMBean = classMBeanServer.getMethod("registerMBean", new Class[] { Object.class,
          classObjectName });
      Object mbs = methodGetPlatformMBeanServer.invoke((Object) null, (Object[]) null);
      Object oName = constructorObjectName.newInstance(new Object[] { name });
      methodRegisterMBean.invoke(mbs, new Object[] { mbean, oName });

      _log.info("Registered MBean with Platform MBean Server:" + name);
    } catch (Throwable t) {
      if (t instanceof ClassNotFoundException) {
        _log.error("Using MBean requires at least a JVM version 1.5.");
      }
      _log.error("Unable to register the " + name + " MBean.");
      t.printStackTrace();
    }
  }

}
