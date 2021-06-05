package cn.shen.testzookeeper.curatorGet;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorGet
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/2/3 9:43 下午
 * @since 1.x
 */
public class CuratorGet {
    /**
     * zk连接地址
     */
    private String connectString = "192.168.0.109:2181";

    /**
     * 会话超时时间
     */
    private int sessionTimeout = 5000;

    /**
     * curator框架的zk连接对象
     */
    CuratorFramework client = null;
    RetryPolicy retryPolicy = new RetryUntilElapsed(1000, 3);

    /**
     * 建立连接
     */
    @Before
    public void conn() {
        //--创建连接对象--//
        client = CuratorFrameworkFactory.builder()
                //--zk地址--//
                .connectString(connectString)
                //--会话超时时间--//
                .sessionTimeoutMs(sessionTimeout)
                //--超时重连策略--//
                .retryPolicy(retryPolicy)
                //--命令空间（此连接创建的节点/curator/xxx）--//
                .namespace("curator")
                //--构建连接对象--//
                .build();

        //--打开连接--//
        client.start();
    }

    /**
     * zk断开连接
     */
    @After
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * 普通读取节点数据
     */
    @Test
    public void test1() {
        byte[] bys = null;
        try {
            bys = client.getData()
                    .forPath("/get");
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (bys != null) {
            System.out.println(new String(bys));
        }
    }

    @Test
    public void test2() {
        byte[] bys = null;
        Stat stat = new Stat();
        try {
            bys = client.getData()
                    .storingStatIn(stat)
                    .forPath("/get");
        } catch (Exception e) {
            e.printStackTrace();
        }
        //--读出数据，stat才有意义--//
        if (bys != null) {
            System.out.println(stat.getVersion());
            System.out.println(new String(bys));
        }
    }

    /**
     * 异步
     */
    @Test
    public void test4() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            client.getData()
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
                            //--事件--//
                            System.out.println(curatorEvent.getType());
                            //--路径--//
                            System.out.println(curatorEvent.getPath());
                            //--数据--//
                            byte[] bys = curatorEvent.getData();
                            System.out.println(new String(bys));
                            //--状态--//
                            System.out.println(curatorEvent.getStat().getVersion());
                            countDownLatch.countDown();
                        }
                    })
                    .forPath("/get");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("结束");
    }
}
