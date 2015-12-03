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
package org.apache.pig.backend.hadoop;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.tools.pigstats.ScriptState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ATSService {
    static public class ATSEvent {
        public ATSEvent(String pigAuditId, String callerId) {
            this.pigScriptId = pigAuditId;
            this.callerId = callerId;
        }
        String callerId;
        String pigScriptId;
    }
    private static final Log log = LogFactory.getLog(ATSService.class.getName());
    static ATSService instance;
    private static final Object LOCK = new Object();
    private static ExecutorService executor;
    private TimelineClient timelineClient;
    public static final String EntityType = "PIG_SCRIPT_ID";
    public static final String EntityCallerId = "callerId";
    public static final String CallerContext = "PIG";
    public static final int AUDIT_ID_MAX_LENGTH = 128;
    private static final int WAIT_TIME = 3;

    public static ATSService getInstance() {
        if (instance==null) {
            instance = new ATSService();
        }
        return instance;
    }

    private ATSService() {
        synchronized(LOCK) {
            if (executor == null) {
                executor = Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ATS Logger %d").build());
                YarnConfiguration yarnConf = new YarnConfiguration();
                timelineClient = TimelineClient.createTimelineClient();
                timelineClient.init(yarnConf);
                timelineClient.start();
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        timelineClient.stop();
                        executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                        executor = null;
                    } catch(InterruptedException ie) { /* ignore */ }
                    timelineClient.stop();
                }
            });
        }
        log.info("Created ATS Hook");
    }

    static public String getPigAuditId(PigContext context) {
        String auditId;
        if (context.getProperties().get(PigImplConstants.PIG_AUDIT_ID) != null) {
            auditId = (String)context.getProperties().get(PigImplConstants.PIG_AUDIT_ID);
        } else {
            ScriptState ss = ScriptState.get();
            String filename = ss.getFileName().isEmpty()?"default" : new File(ss.getFileName()).getName();
            auditId = CallerContext + "-" + filename + "-" + ss.getId();
        }
        return auditId.substring(0, Math.min(auditId.length(), AUDIT_ID_MAX_LENGTH));
    }

    synchronized public void logEvent(final ATSEvent event) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                TimelineEntity entity = new TimelineEntity();
                entity.setEntityId(event.pigScriptId);
                entity.setEntityType(EntityType);
                entity.addPrimaryFilter(EntityCallerId, event.callerId!=null?event.callerId : "default");
                try {
                    timelineClient.putEntities(entity);
                } catch (Exception e) {
                    log.info("Failed to submit plan to ATS: " + e.getMessage());
                }
            }
        });
    }
}
