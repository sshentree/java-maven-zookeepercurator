package cn.shen.testzookeeper.curatorGetChildren;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.retry.RetryUntilElapsed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorGetChildren
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/2/3 11:41 下午
 * @since 1.x
 */
public class CuratorGetChildren {
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
//                .namespace("curator")
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
     * 普通获取子节点
     */
    @Test
    public void test1() {
        List<String> childrenNode = null;
        try {
            childrenNode = client.getChildren()
                    .forPath("/curator");
        } catch (Exception e) {
            e.printStackTrace();
        }
        //--遍历子节点--//
        if (childrenNode != null) {
            for (String node: childrenNode) {
                System.out.println(node);
            }
        }
    }

    /**
     * 异步
     */
    @Test
    public void test2() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            client.getChildren()
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
                            //--事件--//
                            System.out.println(curatorEvent.getType());
                            //--路径--//
                            System.out.println(curatorEvent.getPath());
                            //--子节点--//
                            System.out.println(curatorEvent.getChildren());
                            countDownLatch.countDown();
                        }
                    })
                    .forPath("/curator");
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
