package com.wangwren.config;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * 通用的watch类
 *
 * 包括callback回调，都是异步方式的
 *
 * 用做分布式配置
 */
public class WatchCallBack implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {

    /**
     * zk对象，由使用WatchCallBack的人传入zk
     */
    private ZooKeeper zk;

    /**
     * 配置对象，用于保存zk中获取到的数据，也由调用WatchCallBack的人传入
     */
    private MyConf myConf;

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public MyConf getMyConf() {
        return myConf;
    }

    public void setMyConf(MyConf myConf) {
        this.myConf = myConf;
    }

    private CountDownLatch latch  = new CountDownLatch(1);

    /**
     * 对外只提供这个方法
     */
    public void await(){
        //先判断节点是否存在，以下代码中全部通过异步回调的方式获取返回值
        zk.exists("/AppConf",this,this,"exits");

        //因为是回调，所以这里要进行等待
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * exits的回调callback调用这个
     * @param rc
     * @param path
     * @param ctx
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (stat != null){
            //当stat不为空，即exits的节点存在，存在就获取数据，也通过callback的方式
            zk.getData("/AppConf",this,this,"getData");
        }
    }

    /**
     * exits，getData的watch事件调这个
     *
     * 节点创建、删除、修改时
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                //如果是新创建的节点，没有必要判断是否存在了，直接获取就完事
                zk.getData("/AppConf",this,this,"getData");
                break;
            case NodeDeleted:
                //节点既然能被删除，那就一定是存在的。如果节点被删除了，看你想怎么处理了
                //这里做容忍性处理，将数据置为空
                myConf.setConf("");

                //节点之前就已经存在了，这里需要重新初始化latch，在获取数据时重新countDown，否则就一直阻塞了。
                latch = new CountDownLatch(1);
                break;
            case NodeDataChanged:
                //当数据修改的时候，也要重新获取
                zk.getData("/AppConf",this,this,"getData");
                break;
            case NodeChildrenChanged:
                break;
        }
    }

    /**
     * getData 时的callback事件调用这个方法
     * @param rc
     * @param path
     * @param ctx
     * @param data
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if (data != null){
            //如果数据不为空
            String str = new String(data);
            myConf.setConf(str);

            //latch减1，子线程已经做完了，主线程不用等待了
            latch.countDown();
        }
    }
}
