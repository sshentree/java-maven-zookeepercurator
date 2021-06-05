package cn.shen.testzookeeper.curatorConnection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Test;

/**
 * @author shendezheng
 * @version 1.0
 * @className CuratorConn
 * @description TODO
 * @email ShenDeZ@163.com
 * @date 2021/1/31 9:16 下午
 * @since 1.x
 */
public class CuratorConn {
    /**
     * zk连接地址
     */
    private String connectString = "192.168.0.106:2181";

    /**
     * 会话超时时间
     */
    private int sessionTimeout = 5000;

    @Test
    public void test1() {
        //--创建连接对象--//
        CuratorFramework client = CuratorFrameworkFactory.builder()
                //--zk地址--//
                .connectString(connectString)
                //--会话超时时间--//
                .sessionTimeoutMs(sessionTimeout)
                //--超时重连策略（超时3秒，重连一次）--//
                .retryPolicy(new RetryOneTime(3000))
                //--命令空间（此连接创建的节点/cyrator/xxx）--//
                .namespace("curator")
                //--构建连接对象--//
                .build();

        //--打开连接--//
        client.start();

        //--方法过时--//
        //--判断连接是否打开（true打开、false关闭）--//
//        if (client.isStarted()) {
//            System.out.println("连接打开");
//        } else {
//            System.out.println("连接关闭");
//        }

        //--判断是否连接成功（name == STARTED）--//
        CuratorFrameworkState state = client.getState();
        System.out.println(state.name());

        //--关闭连接--//
        client.close();
    }
}
