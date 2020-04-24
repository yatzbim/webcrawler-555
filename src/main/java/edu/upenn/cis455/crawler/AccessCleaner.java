package edu.upenn.cis455.crawler;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class AccessCleaner extends Thread {
    
    public static AtomicBoolean flag = new AtomicBoolean(true);
    
    XPathCrawler instance = XPathCrawler.getInstance();
    
    // TODO: IMPORTANT - value stored in last access map should be (new Date().getTime() + delay)
    
    @Override
    public void run() {
        while (flag.get()) {
            for (String hostPort: instance.lastAccessed.keySet()) {
                if (instance.lastAccessed.get(hostPort) < new Date().getTime()) {
                    instance.lastAccessed.remove(hostPort);
                }
            }
            try {
                Thread.sleep(45000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
