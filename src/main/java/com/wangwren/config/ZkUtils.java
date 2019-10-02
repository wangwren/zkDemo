package com.wangwren.config;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * zookeeper连接工具
 * @author wangwren
 */
public class ZkUtils {

    public static ZooKeeper zk;

    /**
     * 连接的集群地址
     * 最后指定了其根目录，这个目录必须要事先创建出来
     *
     * 以后客户端会直接在这个节点下进行操作，即testConf成了根目录
     */
    public static String address = "192.168.31.20:2181,192.168.31.21:2181,192.168.31.22:2181,192.168.31.23:2181/testConf";

    /**
     * 创建zk时的自定义watch
     */
    public static DefaultWatcher watcher = new DefaultWatcher();

    /**
     * 门闩 ，在自定义的watch中，zk创建完成后会latch.countDown()
     */
    public static CountDownLatch latch = new CountDownLatch(1);

    public static ZooKeeper getZk(){

        try {

            //把latch赋给watch，好让同一个对象在不同的对象中持有引用
            watcher.setLatch(latch);

            //创建zk
            zk = new ZooKeeper(address,1000,watcher);

            //由于zk的创建是异步的，所以必须要等待zk创建完成后，才能拿到完整的zk对象
            latch.await();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return zk;
    }
}
