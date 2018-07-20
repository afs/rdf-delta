/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.zk;

import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.lib.JSONX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Zookeeper and Curator client operations.
 * @see ZkS
 */
public class Zk {
    private final static Logger LOG = LoggerFactory.getLogger(Zk.class);
    
    private static void zkException(String caller, String path, Exception ex) {
        String msg = String.format("%s[%s] ZooKeeper exception: %s", caller, path, ex.getMessage(), ex);
        LOG.warn(msg, ex);
    }
    
    /** Connect a curator client to a running ZooKepper server. */
    public static CuratorFramework curator(String connectString) {
        try {
            RetryPolicy policy = new ExponentialBackoffRetry(10000, 5);
            CuratorFramework client = 
                CuratorFrameworkFactory.newClient(connectString, 10000, 10000, policy);
//                CuratorFrameworkFactory.builder()
//                //.namespace("delta")
//                .connectString(connectString)
//                //.connectionHandlingPolicy(ConnectionHandlingPolicy.)
//                .retryPolicy(policy)
//                .build();
            client.start();
            //client.getConnectionStateListenable().addListener((c, newState)->System.out.println("** STATE CHANGED TO : " + newState));
            client.blockUntilConnected();
            return client;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private static void zkException(Exception ex) {
        LOG.warn("ZooKeeper exception: "+ex.getMessage(), ex);
    }

    public static String zkPath(String root, String c, String...components) {
        return ZKPaths.makePath(root, c, components);
    }

    public static boolean zkExists(CuratorFramework client, String path) {
        return zkCalc(()->client.checkExists().forPath(path)!=null);
    }
    
    public static String zkEnsure(CuratorFramework client, String path) {
        try {
            ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), path, true);
            return path;
        } catch (Exception e) {
            LOG.error("Failed: mkdirs("+path+")",e) ;
            return null;
        }
    }
    
    public static String zkFetchStr(CuratorFramework client, String path) {
        byte[] x = zkFetch(client, path);
        if ( x == null )
            return null;
        return StrUtils.fromUTF8bytes(x);
    }
    
    public static JsonObject zkFetchJson(CuratorFramework client, String path) {
        return zkFetchJson(client, null, path);
    }

    public static JsonObject zkFetchJson(CuratorFramework client, Watcher watcher, String path) {
        byte[] x = zkFetch(client, watcher, path);
        if ( x == null )
            return null;
        if ( x.length == 0 )
            return null;
        return JSONX.fromBytes(x);
    }

    public static byte[] zkFetch(CuratorFramework client,  String path) {
        return zkFetch(client, null, path);
    }
    
    public static byte[] zkFetch(CuratorFramework client, Watcher watcher, String path) {
        try {
            GetDataBuilder b = client.getData();
            if ( watcher != null )
                b.usingWatcher(watcher);
            return b.forPath(path);
        } catch (IllegalArgumentException ex) {
            return null;
        } catch (Exception ex) {
            //LOG.warn("Failed: zkFetch(" + path + ") " + ex.getMessage());
            return null;
        }
    }
    
    /** Return a list of the children of the node - the names are the sub zNode names, not paths */  
    public static List<String> zkSubNodes(CuratorFramework client, String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            LOG.error("Failed: zkSubNodes("+path+")",e) ;
            return null;
        }
    }

    public static void zkCreateSet(CuratorFramework client, String path, byte[]bytes) {
        zkRun(()->client.create().forPath(path, bytes));
    }

    public static void zkCreate(CuratorFramework client, String path) {
        zkCreate(client, path, CreateMode.PERSISTENT);
    }

    private static void zkCreate(CuratorFramework client, String path, CreateMode mode) {
        zkCreateSet(client, path, new byte[0]);
    }

    /** Delete this znode and all nodes below it. */
    public static void zkDelete(CuratorFramework client, String path) {
        zkRun(()->{
            client.delete().deletingChildrenIfNeeded().forPath(path);
        });
    }

    public static void zkLock(CuratorFramework client, String nLock, Runnable action) {
        InterProcessLock lock = new InterProcessSemaphoreMutex(client, nLock);
        try {
            lock.acquire();
            if ( ! lock.isAcquiredInThisProcess() ) 
                LOG.warn("zkLock: lock.isAcquiredInThisProcess() is false");
            action.run();
        } 
        catch (DeltaException ex) { throw ex; }
        catch (Exception ex) {
            zkException("zkLock", nLock, ex);
        } finally {
            try { lock.release(); } catch (Exception ex) {}
        }
    }
    
    public static <X> X zkLockRtn(CuratorFramework client, String nLock, Supplier<X> action) {
        InterProcessLock lock = new InterProcessSemaphoreMutex(client, nLock);
        try {
            lock.acquire();
            if ( ! lock.isAcquiredInThisProcess() ) 
                LOG.warn("zkLockRtn: lock.isAcquiredInThisProcess() is false");
            return action.get();
        }
        catch (DeltaException ex) { throw ex; }
        catch (Exception ex) {
            zkException("zkLock", nLock, ex);
            return null; 
        } finally {
            try { lock.release(); } catch (Exception ex) {}
        }
    }


    // XXX Do we need these?
    
    @FunctionalInterface
    public interface ZkRunnable { public void run() throws Exception; }
    
    @FunctionalInterface
    public interface ZkSupplier<X> { public X run() throws Exception; }
    
    public static void zkRun(ZkRunnable action) {
        try {
            action.run();
        } catch (Exception ex) {
            zkException(ex);
        }
    }
    
    public static <X> X zkCalc(ZkSupplier<X> action) {
        try {
            return action.run();
        } catch (Exception ex) {
            zkException(ex);
            return null;
        }
    }
    
    /** Set an existing zNode to the the bytes for the JSON object */
    public static void zkSetJson(CuratorFramework client, String statePath, JsonObject x) {
        byte[] bytes = JSONX.asBytes(x);
        zkSet(client, statePath, bytes);
    }

    /** Create and set a new zNode: the zNode must not exist before this operation. */
    public static void zkCreateSetJson(CuratorFramework client, String statePath, JsonObject x) {
        byte[] bytes = JSONX.asBytes(x);
        zkCreateSet(client, statePath, bytes);
    }

    public static void zkSet(CuratorFramework client, String p, byte[] b) {
        zkRun(()-> { 
            Stat stat = client.setData().forPath(p, b);
            if ( stat == null )
                LOG.warn("Did not set: "+p);
        });
    }

    public static void listNodes(CuratorFramework client) {
        listNodes(client, "/");
    }
    
    public static void listNodes(CuratorFramework client, String path) {
        String initial = path.equals("/") ? "" : path;
        System.out.printf(">> Path=%s\n", path);
        zkRun(()->recurse(client, path, initial, 1));
        System.out.printf("<< Path=%s\n", path);
    }
    
    private static void recurse(CuratorFramework client, String path, String initial, int depth) {
        try {
            client.getChildren().forPath(path).forEach(p->{
                String x = ZKPaths.makePath(path, p);
                String spc = StringUtils.repeat(" ", 2*(depth-1));
                // Indented, with level number
                //System.out.printf("%-2d %s /%s\n", depth, spc, p);
                // Indented
                //System.out.printf("   %s /%s\n", spc, p);
                // Path below start.
                String print = x.startsWith(initial) ? x.substring(initial.length()) : x;
                System.out.printf("   %s\n", print);
                recurse(client, x, initial, depth+1);
            });
        }
        catch (Exception ex) { return ; }
    }
    

}
