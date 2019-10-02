package com.wangwren.locks;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 分布式锁
 *
 * @author wangwren
 */
public class TestLocks {

    private ZooKeeper zk;

    @Before
    public void conn(){
        zk = ZkLockUtils.getZk();
    }



    @Test
    public void locksTest(){

        //模拟十个线程
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                WatchCallBackLocks locks = new WatchCallBackLocks();

                locks.setZk(zk);
                String name = Thread.currentThread().getName();

                locks.setThreadName(name);

                //争抢锁
                locks.tryLock();


                //获取到锁，才能干活
                System.out.println(name + "干干干...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //释放锁
                locks.unLock();

            }).start();
        }

        while (true){

        }
    }




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
