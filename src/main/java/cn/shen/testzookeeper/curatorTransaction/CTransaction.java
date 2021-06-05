package cn.shen.testzookeeper.curatorTransaction;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorTransaction
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/2/18 7:23 下午
 * @since 1.x
 */
public class CTransaction {
    /**
     * zk连接地址
     */
    private String connectString = "192.168.1.10:2181";

    /**
     * 会话超时时间
     */
    private int sessionTimeout = 5000;

    /**
     * curator框架的zk连接对象
     */
    CuratorFramework client = null;

    /**
     * curator框架连接zk策略
     */
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
     * curator事物测试
     */
    @Test
    public void test1() {
    }
}
