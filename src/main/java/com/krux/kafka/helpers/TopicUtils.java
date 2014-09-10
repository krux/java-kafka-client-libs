package com.krux.kafka.helpers;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class TopicUtils {
    
    public static List<String> getAllTopics( String zkUrl ) {
        
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient( zkUrl, retryPolicy );
        client.start();

        try {
            List<String> children = client.getChildren().forPath("/config/topics/");
            
            return children;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            client.close();
        }
    }

}
