package cn.shen.testzookeeper.curatorWatcher;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.RetryUntilElapsed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorWatcher
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/2/4 9:39 下午
 * @since 1.x
 */
public class CuratorWatcher {
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
    public void test1() {
        CuratorCache curatorCache = CuratorCache.build(client, "/watcher");
        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData childData, ChildData childData1) {
                if (type.name().equals("NODE_CREATED")) {
                    //--节点创建--//
                    System.out.println(childData1.getPath() + " 节点被创建");
                } else if (type.name().equals("NODE_CHANGED")) {
                    //--更新--//
                    System.out.println(type.name() + " 节点数据修改");
                    System.out.println(childData.getPath());
                    if (childData.getData() != null) {
                        System.out.println("修改之前数据" + new String(childData.getData()));
                    } else {
                        System.out.println("节点第一次赋值");
                    }
                    System.out.println("修改之后数据" + new String(childData1.getData()));
                } else {
                    //--节点被删除--//
                    System.out.println(childData.getPath() + " 节点删除");
                    System.out.println(new String(childData.getData()));
                }
            }
        });
        //--启动监视器--//
        curatorCache.start();
        //--关闭监视器--//
//        curatorCache.close();

        while (true) {

        }
    }
}
