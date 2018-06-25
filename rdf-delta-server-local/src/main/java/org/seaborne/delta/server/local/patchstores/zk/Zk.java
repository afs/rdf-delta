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

package org.seaborne.delta.server.local.patchstores.zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Zk {
    private final static Logger LOG = LoggerFactory.getLogger(Zk.class);
    
    private static void zkException(String caller, String path, Exception ex) {
        String msg = String.format("%s[%s] ZooKeeper exception: %s", caller, path, ex.getMessage(), ex);
        LOG.warn(msg, ex);
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
        }
        catch (Exception e) {
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
        byte[] x = zkFetch(client, path);
        if ( x == null )
            return null;
        if ( x.length == 0 )
            return null;
        
        String s = StrUtils.fromUTF8bytes(x);
        return JSON.parse(new ByteArrayInputStream(x));
    }

    public static byte[] zkFetch(CuratorFramework client, String path) {
        try {
            byte[] x = client.getData().forPath(path);
            return x;
        } catch (IllegalArgumentException ex) {
            return null;
        } catch (Exception ex) {
            LOG.error("Failed: zkFetch(" + path + ") " + ex.getMessage(), ex);
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

    public static void zkCreate(CuratorFramework client, String path, CreateMode mode) {
        zkCreateSet(client, path, new byte[0]);
    }

    /** Delete this znode and all nodes below it. */
    public static void zkDelete(CuratorFramework client, String path) {
        zkRun(()->{
            client.delete().deletingChildrenIfNeeded().forPath(path);
        });
    }

    public static void zkLock(CuratorFramework client, String nLock, ZkRunnable action) {
        InterProcessLock lock = new InterProcessSemaphoreMutex(client, nLock);
        try {
            lock.acquire();
            if ( ! lock.isAcquiredInThisProcess() ) {}
            
            try {
                action.run();
            } finally {
                lock.release();
            }
        } catch (Exception ex) {
            zkException("zkLock", nLock, ex);
        }
    }

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
    
    public static void zkSetJson(CuratorFramework client, String statePath, JsonObject x) {
        // XXX Better? Direct JSON to bytes. / Jena.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JSON.write(out, x); 
        zkSet(client, statePath, out.toByteArray());
    }

    public static void zkSet(CuratorFramework client, String p, byte[] b) {
        zkRun(()-> { 
            Stat stat = client.setData().forPath(p, b);
            if ( stat == null )
                LOG.warn("Did not set: "+p);
        });
        
    }

    public static long zkGetNext(CuratorFramework client, String p, byte[] b) {
        System.err.println("zkGetNext");
        throw new NotImplemented();
        //return -1;
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
