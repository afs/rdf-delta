/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.cmds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.jena.atlas.json.JsonException;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.web.HttpException;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.apache.jena.rdfpatch.PatchHeader;
import org.apache.jena.rdfpatch.RDFPatch;

/** Listen to a patch log and print what's happening. */
public class monitor extends DeltaCmd {


    public static void main(String... args) {
        new monitor(args).mainRun();
    }

    private monitor(String... argv) {
        super(argv);
        super.add(argLogName);
        //super.add(argDataSourceURI);
    }

    @Override
    protected void checkForMandatoryArgs() {
//        if ( ! contains(argServer) )
//            throw new CmdException("Required: --server URL");

//        if ( ! contains(argLogName) ) {
//            throw new CmdException("Required: --log NAME");
//        }
    }

    @Override
    protected String getSummary() {
        return null;
    }

    @Override
    protected void execCmd() {
        Monitor monitor = new Monitor(super.dLink, new PrintReactor());
        monitor.start();
        for ( ;; )  {
            boolean success = monitor.runOnce();
            if ( ! success )
                monitor.finish();
            Lib.sleep(2000);
        }
    }

    interface MonitorReactor {
        void onStart(Map<Id, PatchLogInfo> state);
        void onFinish(Map<Id, PatchLogInfo> state);
        void onRename(DeltaLink dLink, PatchLogInfo oldInfo, PatchLogInfo newInfo);
        void onCreate(DeltaLink dLink, PatchLogInfo info);
        void onDelete(DeltaLink dLink, DataSourceDescription dsd);
        void onReset(DeltaLink dLink, PatchLogInfo oldInfo, PatchLogInfo newInfo);
        boolean onChange(DeltaLink dLink, PatchLogInfo oldInfo, PatchLogInfo newInfo);
    }

    static class Monitor {
        private final Object lock = new Object();
        private final Map<Id, PatchLogInfo> state;
        private final DeltaLink dLink;
        private final MonitorReactor reactor;

        public Monitor(DeltaLink deltaLink, MonitorReactor reactor) {
            this.dLink = deltaLink;
            this.reactor = reactor;
            state = getCurrentState(deltaLink);
        }

        private static Map<Id, PatchLogInfo> getCurrentState(DeltaLink dLink) {
            Map<Id, PatchLogInfo> logs = new ConcurrentHashMap<>();
            dLink.listPatchLogInfo().forEach(info-> logs.put(info.getDataSourceId(), info) );
            return logs;
        }

        public void start() {
            reactor.onStart(state);
        }

        public void finish() {
            reactor.onFinish(state);
        }

        public boolean runOnce() {
            try {
                synchronized(lock) {
                    Map<Id, PatchLogInfo> state2 = getCurrentState(dLink);
                    List<PatchLogInfo> added = new ArrayList<>();
                    List<DataSourceDescription> deleted = new ArrayList<>();
                    List<Pair<PatchLogInfo,PatchLogInfo>> renamed = new ArrayList<>();
                    // Weird stuff - version has gone backwards.
                    List<Pair<PatchLogInfo,PatchLogInfo>> reset = new ArrayList<>();

                    List<Pair<PatchLogInfo,PatchLogInfo>> changed = new ArrayList<>();
                    List<PatchLogInfo> noChange = new ArrayList<>();

                    // Calculate deleted
                    state.forEach((dsRef,info)->{
                        if ( ! state2.containsKey(dsRef) ) {
                            DataSourceDescription dsd = info.getDataSourceDescr();
                            deleted.add(dsd);
                        }
                    });

                    // Calculate created, renamed, reset, changed and unchanged.
                    state2.forEach((id,newInfo)->{
                        PatchLogInfo oldInfo = state.get(id);

                        if ( ! state.containsKey(id) ) {
                            added.add(newInfo);
                            return;
                        }

                        // Existing log.
                        if ( ! oldInfo.getDataSourceName().equals(newInfo.getDataSourceName()) )
                            renamed.add(Pair.create(oldInfo,newInfo));

                        long oldVersion = oldInfo.getMaxVersion().value();
                        long newVersion = newInfo.getMaxVersion().value();

                        if ( oldVersion == newVersion ) {
                            noChange.add(newInfo);
                            if ( ! oldInfo.equals(newInfo) )
                                System.err.printf("Inconsistent: %s %s\n", oldInfo, newInfo);
                        } else if ( oldVersion < newVersion )
                            changed.add(Pair.create(oldInfo,newInfo));
                        else {
                            // Went backwards!
                            reset.add(Pair.create(oldInfo,newInfo));
                        }
                    });

                    // Enact changes
                    added.forEach(info->state.put(info.getDataSourceId(), info));
                    deleted.forEach(dsd->state.remove(dsd.getId()));
                    renamed.forEach(pair->state.put(pair.getRight().getDataSourceId(), pair.getRight()));
                    reset.forEach(pair->{
                        PatchLogInfo newInfo = pair.getRight();
                        state.put(newInfo.getDataSourceId(), newInfo);
                    });

                    // Report changes
                    added.forEach(info->reactor.onCreate(dLink, info));
                    deleted.forEach(dsd->reactor.onDelete(dLink, dsd));
                    renamed.forEach(pair->reactor.onRename(dLink, pair.getLeft(), pair.getRight()));
                    reset.forEach(pair->reactor.onReset(dLink, pair.getLeft(), pair.getRight()));

                    changed.forEach(pair->{
                        PatchLogInfo oldInfo = pair.getLeft();
                        PatchLogInfo newInfo = pair.getRight();
                        reactor.onChange(dLink, oldInfo, newInfo);
                        state.put(newInfo.getDataSourceId(), newInfo);
                    });

                    // noChange

                    return true;
                }
            } catch (JsonException | DeltaException | HttpException ex) {
                return false;
            }
        }
    }

    static class PrintReactor implements MonitorReactor {

        @Override
        public void onStart(Map<Id, PatchLogInfo> state) {
            if ( state.isEmpty() )
                System.out.println("No logs");
            else
                state.forEach((name,info)->System.out.println("Log: "+info));
        }

        @Override
        public void onFinish(Map<Id, PatchLogInfo> lastState) {
            System.exit(0);
        }

        @Override
        public void onRename(DeltaLink dLink, PatchLogInfo oldInfo, PatchLogInfo newInfo) {
            System.out.println("Renamed: "+oldInfo.getDataSourceName()+ " => "+newInfo.getDataSourceName());
        }

        @Override
        public void onCreate(DeltaLink dLink, PatchLogInfo info) {
            System.out.println("Created: "+info);
        }

        @Override
        public void onDelete(DeltaLink dLink, DataSourceDescription dsd) {
            System.out.println("Deleted: "+dsd);
        }

        @Override
        public void onReset(DeltaLink dLink, PatchLogInfo oldInfo, PatchLogInfo newInfo) {
            System.out.println("Reset: "+oldInfo+ " => "+newInfo);
        }

        /** onChange. This operation may contact the server; the "newInfo" may be out of date.
         * An exception should be thrown if the server can not be contacted.
         */
        @Override
        public boolean onChange(DeltaLink dLink, PatchLogInfo oldInfo, PatchLogInfo newInfo) {
            try {
                onChangeDescribe(dLink, oldInfo, newInfo);
                return true;
            } catch (JsonException | DeltaException | HttpException ex) {
                return false;
            }
        }

        protected void onChangeDescribe(DeltaLink dLink, PatchLogInfo oldState, PatchLogInfo newState) {
            Version headVersion = newState.getMaxVersion();
            if ( ! headVersion.isValid() )
                return;
            String name = oldState.getDataSourceDescr().getName();
            Id dsRef = oldState.getDataSourceId();

            play(oldState, newState, (verObj)->{
                RDFPatch patch = dLink.fetch(dsRef, verObj);
                if ( patch == null )
                    throw new DeltaNotFoundException("No patch at version "+verObj);
                PatchHeader header = patch.header();
                header.forEach((h,v)->System.out.printf("%s [%s, %s]\n", verObj, h, header.get(h)));
            });
        }

        protected void play(PatchLogInfo oldState, PatchLogInfo newState, Consumer<Version> action) {
            // Extract: "meta-play"
            long minVersion = Math.max(0,oldState.getMaxVersion().value());
            long maxVersion = newState.getMaxVersion().value();
            for ( long ver = minVersion+1 ; ver <= maxVersion ; ver++ ) {
                Version verObj = Version.create(ver);
                action.accept(verObj);
            }
        }
    }
}
