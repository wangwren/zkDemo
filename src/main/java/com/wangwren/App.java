package com.wangwren;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;


/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws Exception {
        System.out.println( "Hello World!" );

        CountDownLatch latch = new CountDownLatch(1);

        /**
         * 创建一个zookeeper
         *
         * zookeeper是有session概念的，没有连接池的概念
         *
         * new一个zookeeper，第一个参数是集群的ip，并指定端口号，集群之间用逗号隔开；
         * 第二个参数代表session创建出的数据的过期时间，单位毫秒，即当session退出时，zookeeper给它创建的数据还保留3秒钟
         * 第三个参数创建一个watch监听，这个watch是session级别的，跟path、node没关系，只是在创建zookeeper时监听
         */
        ZooKeeper zk = new ZooKeeper("192.168.0.20:2181,192.168.0.21:2181,192.168.0.22:2181,192.168.0.22:2181"
                , 3000
                , new Watcher() {
                    /**
                     * 重写watch的回调方法
                     *
                     * 程序都是顺序执行的，这种回调方法像是单独开了一个线程，由别人调用的，
                     * 所以想要等zookeeper正在创建完成，需要在主程序中等待
                     */
                    @Override
                    public void process(WatchedEvent event) {
                        //zookeeper创建时的状态
                        Event.KeeperState state = event.getState();
                        //zookeeper创建时的类型
                        Event.EventType type = event.getType();

                        System.out.println("new zk " + event.toString());

                        switch (state) {
                            case Unknown:
                                break;
                            case Disconnected:
                                break;
                            case NoSyncConnected:
                                break;
                            case SyncConnected:
                                System.out.println("new zk connected...");
                                latch.countDown();
                                break;
                            case AuthFailed:
                                break;
                            case ConnectedReadOnly:
                                break;
                            case SaslAuthenticated:
                                break;
                            case Expired:
                                break;
                        }

                        switch (type) {
                            case None:
                                break;
                            case NodeCreated:
                                break;
                            case NodeDeleted:
                                break;
                            case NodeDataChanged:
                                break;
                            case NodeChildrenChanged:
                                break;
                        }
                    }
                });

        //等待zk创建，如果这里不等待，那么程序顺序执行，可能输出的结果就是connecting，zk还没创建完呢，怎么继续使用？
        latch.await();

        //获取zk的创建状态
        ZooKeeper.States state = zk.getState();

        switch (state) {
            case CONNECTING:
                System.out.println("connecting...");
                break;
            case ASSOCIATING:
                break;
            case CONNECTED:
                System.out.println("connected...");
                break;
            case CONNECTEDREADONLY:
                break;
            case CLOSED:
                break;
            case AUTH_FAILED:
                break;
            case NOT_CONNECTED:
                break;
        }


        //简单使用

        /*
          创建节点
          第一个参数：节点名称
          第二个参数：数据，zookeeper也是二进制安全的，与Redis一样
          第三个参数：权限相关
          第四个参数：创建的节点类型，即 -s -e 这里创建的是-e，临时节点类型

          还有一种创建节点，方法名都一样，只是带了另一个参数:回调方法
        */
        String pathName = zk.create("/xxoo", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        //输出的结果就是 /xxoo
        System.out.println("创建的节点名称：" + pathName);


        /*
          获取数据
          第一个参数：指定路径
          第二个参数：指定监听，watch的注册发生在读的时候和exits(判断是否存在)
                    而且watch监听只会触发一次，如果想多次触发，那么可以在回调方法中重新注册
          第三个参数：指定节点元数据信息
         */
        Stat stat = new Stat();

        //返回值是该节点上的数据
        byte[] data = zk.getData(pathName, new Watcher() {
            //获取节点时指定的监听回调方法，该方法只有在节点被修改、删除、子节点被更改时会被调用
            @Override
            public void process(WatchedEvent event) {
                try {

                    System.out.println("修改节点时被触发...");

                    /*
                      重新注册了watch，注意第二个参数，该参数可以传入一个boolean变量的值，true表示使用 new zk 时的那个watch
                      写this就是表示当前对象，即当前的watch，又被注册了一遍

                      如果不写下面这行代码，那么在第二次修改数据时watch中的回调方法将不会再被调用
                     */
                    zk.getData(pathName,this,stat);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, stat);

        System.out.println("获取节点上的数据：" + new String(data));

        //触发节点上的回调，最后一个参数：指定版本，当版本与当前要修改的版本不同时是不允许修改的会报错；但是写-1就随便修改
        Stat stat1 = zk.setData(pathName, "hello world".getBytes(), -1);

        //再次修改还是可以触发watch回调
        Stat stat2 = zk.setData(pathName, "hello world world".getBytes(), -1);


        //获取数据时还可以使用回调方法，同样的方法，但是需要写回调方法，数据都在回调方法里

        System.out.println("=========async start============");
        /*
          第一个参数：节点
          第二个参数：不适用监听
          第三个参数：指定回调方法
          第四个参数：上下文，传入的是一个object对象
         */
        zk.getData(pathName, false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("=========async callback============");
                System.out.println("rc = " + rc);
                System.out.println("path = " + path);
                System.out.println("ctx = " + ctx);
                System.out.println("data = " + new String(data));
            }
        },zk);

        System.out.println("=========async over============");

        /**
         * 通过输出结果可以看到，确实是回调方法，即start over都打印完了才打印的callback
         * 即程序还是顺序执行了，这种方式会将CPU用到极致，即只有当有消息来的时候才做某一件事，而不是傻等着，这种getData方法也没有返回值
         *
         * 这是异步的方式
         *
         * 所以写了一个死循环阻塞在这
         */

        while (true){

        }
    }
}
