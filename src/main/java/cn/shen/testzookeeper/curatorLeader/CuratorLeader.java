package cn.shen.testzookeeper.curatorLeader;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author shendezheng
 * @version 1.0.0
 * @className CuratorLeader
 * @description TODO
 * @since 2021/7/25 8:50 下午
 **/
public class CuratorLeader {
    /**
     * zk连接地址
     */
    private String connectString = "192.168.0.103:2181";

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
     * 监听某个节点的数据变化
     */
    @Test
    public void test1() throws Exception {
        if ("STARTED".equals(client.getState().name())) {
            System.out.println("zookeeper 连接成功");
        } else {
            System.out.println("zookeeper 连接失败");
            return;
        }

        // 注册主节点
        List<LeaderLatch> leaderLatchArrayList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            LeaderLatch leaderLatch = new LeaderLatch(client, "/leader", i + "");
            leaderLatchArrayList.add(leaderLatch);
            // 添加监听器
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    while (true) {
                        System.out.println(System.currentTimeMillis());
                        try {
                            TimeUnit.SECONDS.sleep(2);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void notLeader() {

                }
            });
            leaderLatch.start();
        }

        // 等待完成抢主
        TimeUnit.SECONDS.sleep(5);
        for (LeaderLatch leaderLatch: leaderLatchArrayList) {
            if (leaderLatch.hasLeadership()) {
                System.out.println("------------------");
                System.out.println(leaderLatch.getId());
                System.out.println("------------------");
            }
        }
        // 获取参与选主的节点集合（分布式一台机器只能记录该机器参与选主的节点，无法获取其他机器参与的节点，
        // 所以使用此方法获取参与选主的所有节点）
        Collection<Participant> participants = leaderLatchArrayList.get(0).getParticipants();
        System.out.println("+++++++++++++++++++");
        System.out.println(participants.size());
        System.out.println("+++++++++++++++++++");

        TimeUnit.HOURS.sleep(1);
    }

}
