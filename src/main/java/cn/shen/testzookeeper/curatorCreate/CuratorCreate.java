package cn.shen.testzookeeper.curatorCreate;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorCreate
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/1/31 11:58 下午
 * @since 1.x
 */
public class CuratorCreate {
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
                //--命令空间（此连接创建的节点/cyrator/xxx）--//
                .namespace("curator")
                //--构建连接对象--//
                .build();

        //--打开连接--//
        client.start();
    }

    /**
     * 创建节点
     */
    @Test
    public void test1() {
        try {
            //--新增节点--//
            client.create()
                    //--节点类型（持久、临时、持久有序、临时有序）--//
                    .withMode(CreateMode.PERSISTENT)
                    //--ACL权限（world:anyone:cdraw）--//
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    //--路径，数据--//
                    .forPath("/node1", "node1".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("结束");
    }

    /**
     * 自定义权限列表
     */
    @Test
    public void test2() {
        String ip = "192.168.0.102";
        //--权限列表--//
        List<ACL> list = new ArrayList<>();
        //--授权模式、授权对象（ip模式）--//
        Id id = new Id("ip", ip);
        //--权限为All--//
        list.add(new ACL(ZooDefs.Perms.ALL, id));

        try {
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(list)
                    .forPath("/node2", "node2".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("结束");
    }

    /**
     * 递归创建节点
     */
    @Test
    public void test3() {

        try {
            client.create()
                    //--递归创建（父节点不存在，则创建）--//
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath("/parent/sun", "sun".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("结束");
    }

    /**
     * 分析递归创建时，父节点的ACL权限设置
     * 结论：父节点与子节点的ACL无关，父节点ACL为默认的 word:anyone:cdraw
     */
    @Test
    public void test4() {
        String ip = "192.168.0.106";
        //--权限列表--//
        List<ACL> list = new ArrayList<>();
        //--授权模式、授权对象（ip模式）--//
        Id id = new Id("ip", ip);
        //--权限为All--//
        list.add(new ACL(ZooDefs.Perms.ALL, id));

        try {
            client.create()
                    //--递归创建（父节点不存在，则创建）--//
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(list)
                    .forPath("/parent1/sun1", "sun1".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("结束");
    }

    /**
     * 异步创建节点
     */
    @Test
    public void test5() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
                            //--连接成功（STARTED）--//
                            System.out.println(curatorFramework.getState().name());
                            //--事件类型--//
                            System.out.println(curatorEvent.getType());
                            //--路径--//
                            System.out.println(curatorEvent.getPath());

                            //--同步器--//
                            countDownLatch.countDown();
                        }
                    })
                    .forPath("/node3", "node3".getBytes());

            //--同步器--//
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("结束");
    }

    /**
     * 断开连接
     */
    @After
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
