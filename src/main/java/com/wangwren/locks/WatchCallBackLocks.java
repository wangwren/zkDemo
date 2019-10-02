package com.wangwren.locks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式锁
 * @author wangwren
 */
public class WatchCallBackLocks implements Watcher,AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {

    private ZooKeeper zk;

    /**
     * 线程名称
     */
    private String threadName;

    /**
     * 线程创建的有序节点名称
     */
    private String pathName;

    private CountDownLatch latch = new CountDownLatch(1);


    /**
     * 争抢锁
     */
    public void tryLock(){
        System.out.println(threadName + " create ...");

        //创建一个临时有序节点lock，数据时线程的名字。注意这里主要是临时有序，有序的可以保证每个线程创建的节点是有序号的
        //剩下操作都在回调里
        zk.create("/lock",threadName.getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL
                ,this,"create lock");

        //等待，等待第一个节点获取到锁，拿到锁之后才开始干活，每一个线程都有一个latch
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * zk创建节点时的回调
     * @param rc
     * @param path
     * @param ctx
     * @param name
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {

        if (name != null){
            System.out.println(threadName + " create node : " + name);

            pathName = name;

            //获取 /testLocks 下的所有子节点，即线程创建出的节点，并且并不需要给/testLocks设置watch事件，watch事件应该设置在子节点上
            zk.getChildren("/",false,this,"gethCildren");
        }
    }

    /**
     * zk获取指定节点下的所有子节点时的回调
     *
     * 最主要的就是这个函数
     *  需要获取到所有的节点，并且进行排序(按照节点名称排序)
     *  排好序后，索引为0，即节点名称最小的节点获得锁
     *  后面的每个节点都watch前边的节点，这样当前面的节点释放锁或者挂了的时候，不需要通知全部，只通知后面一个节点就可以了。
     * @param rc
     * @param path
     * @param ctx
     * @param children 存放的是所有子节点的节点名称
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {

//        System.out.println(threadName + "lock locks...");
//
//        for (String child : children) {
//            System.out.println(child);
//        }

        //排序
        Collections.sort(children);

        //通过输出打印可以看出，pathName这个值是带有 / 的，即 /lock0000000000
        //而children集合中的节点名称是不带 / 的，即 lock0000000000

        //排序好后，截取掉pathName的/ ，获取当前线程的pathName是集合中的第几个索引
        int i = children.indexOf(pathName.substring(1));

        //判断 i 的值
        if (i == 0){
            //如果是第一个，就让第一个拿到锁
            System.out.println(threadName + " i am first...");

            try {
                //加上锁之后，把值给赋值给根目录
                zk.setData("/",threadName.getBytes(),-1);
                latch.countDown();

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            //如果不是第一个，就让现在这个线程去监听前一个节点
            zk.exists("/" + children.get(i -1),this,this,"not first");
        }
    }

    /**
     * exits时的回调
     * @param rc
     * @param path
     * @param ctx
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {

        //这个回调先不写了，但是在生产环境下，必须写，即监听的前一个节点如果发生变化，如果处理，比如说挂了
    }

    /**
     * watch
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                //当节点被删除了，重新获取子节点，并重新排序。会调用回调方法
                zk.getChildren("/",false,this,"gethCildren");
                break;
            case NodeDataChanged:
                break;
            case NodeChildrenChanged:
                break;
        }
    }


    /**
     * 释放锁
     */
    public void unLock(){

        //释放锁就把节点删了就行了
        try {
            //当前哪个pathName都是确定的，因为之前有个加锁的等待，所以不会有不一样的情况
            zk.delete(pathName,-1);

            System.out.println(threadName + "over work...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }



    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

}
