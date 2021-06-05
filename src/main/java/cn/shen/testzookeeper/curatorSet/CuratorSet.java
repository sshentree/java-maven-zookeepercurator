package cn.shen.testzookeeper.curatorSet;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryUntilElapsed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorSet
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/2/3 9:52 下午
 * @since 1.x
 */
public class CuratorSet {
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
     * 普通更新节点数据
     */
    @Test
    public void test1() {
        try {
            client.setData()
                    .forPath("/set", "set".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("结束");
    }

    /**
     * 设置版本号
     */
    @Test
    public void test2() {
        try {
            client.setData()
                    /**
                     * 使用版本号进行验证
                     *  -1：版本号不参与（和没有一样）
                     *  大于和小于当前版本号：修改不成功
                     *  与当前版本号相同：修改成功）
                     */
                    .withVersion(2)
                    .forPath("/set", "set2".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("结束");
    }

    /**
     * 异步更新节点数据
     */
    @Test
    public void test() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            client.setData()
                    .withVersion(-1)
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
                            //--事件类型--//
                            System.out.println(curatorEvent.getType());
                            //--路径--//
                            System.out.println(curatorEvent.getPath());
                            countDownLatch.countDown();
                        }
                    })
                    .forPath("/set", "set3".getBytes());
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
