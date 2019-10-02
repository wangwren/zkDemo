package com.wangwren.config;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * 使用zk 进行分布式配置的设置
 *
 * 整体思路：
 *      1. 先判断指定节点是否存在
 *          1.1 如果节点不存在，那么会一直等待，并且在判断节点是否存在时加上了watch事件
 *          1.2 如果节点存在，在判断节点是否存在的回调中获取节点中的数据，并且指定watch事件和callback事件
 *
 *      2. 在获取数据的callback回调中，获取到数据，并将数据赋值给pojo，并将latch countDown
 *      3. 在watch事件中，监听节点的创建、节点的改变、节点的删除事件，当这些事件发生时，也会做出相应的反应，供使用者使用
 *
 *
 *      具体代码就是在这个config包下的代码，主要是TestConfig.java 和 WatchCallBack.java
 *
 * @author wangwren
 */
public class TestConfig {

    private ZooKeeper zk;

    /**
     * zk连接
     */
    @Before
    public void conn(){
        zk = ZkUtils.getZk();
    }


    @Test
    public void getConf(){

        WatchCallBack watchCallBack = new WatchCallBack();
        watchCallBack.setZk(zk);

        MyConf myConf = new MyConf();
        watchCallBack.setMyConf(myConf);

        //获取配置了
        watchCallBack.await();

        //写个死循环阻塞住主线程
        //当程序zk改变数据时，这里会实时的打印出对应的数据信息
        while (true){
            //如果没有数据，会阻塞在WatchCallBack中

            if (myConf.getConf().equals("")){
                System.out.println("没有配置信息...");

                //当没有数据的时候，可能就是被删除了，即有这个节点但是节点中没数据，那么需要重新判断是否存在节点，让他阻塞住
                //这里的删除操作是我自己定的，删除后conf的值就是""，节点已经没了，所以会被阻塞住
                watchCallBack.await();
            } else {

                //当数据做了修改或者头一次创建出来的时候会触发watch，重新获取数据
                System.out.println("配置信息===" + myConf.getConf());
            }

            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 测试完成后关闭连接
     */
    @After
    public void close(){
        if (zk != null){
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
