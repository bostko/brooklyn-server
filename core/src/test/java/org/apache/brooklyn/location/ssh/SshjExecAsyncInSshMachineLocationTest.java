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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.location.ssh;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.brooklyn.util.core.internal.ssh.ShellTool.PROP_EXEC_ASYNC;
import static org.apache.brooklyn.util.core.internal.ssh.SshTool.BROOKLYN_CONFIG_KEY_PREFIX;
import static org.testng.Assert.assertTrue;

@Test(groups = "WIP")
public class SshjExecAsyncInSshMachineLocationTest extends BrooklynAppUnitTestSupport {
    protected SshMachineLocation host;

    public static class RecordingSshjTool extends SshjTool {
        public static final List<List<String>> commandsExecuted = Lists.newCopyOnWriteArrayList();
        public static List<RecordingSshTool.ExecCmd> execScriptCmds = Lists.newCopyOnWriteArrayList();
        public static List<Map<?,?>> constructorProps = Lists.newCopyOnWriteArrayList();
        public static Map<String, RecordingSshTool.CustomResponseGenerator> customResponses = Maps.newConcurrentMap();

        private boolean connected;

        public RecordingSshjTool(Map<String, ?> map) {
            super(map);
        }

        @Override public void connect() {
            connected = true;
        }
        @Override public void connect(int maxAttempts) {
            connected = true;
        }
        @Override public void disconnect() {
            connected = false;
        }
        @Override public boolean isConnected() {
            return connected;
        }

        @Override
        protected int execScriptAsyncAndPoll(final Map<String, ?> props, final List<String> commands, final Map<String, ?> env) {
            commandsExecuted.add(commands);
            return 0;
        }

        public static void clear() {
            commandsExecuted.clear();
            execScriptCmds.clear();
            constructorProps.clear();
            customResponses.clear();
        }

        public static void setCustomResponse(String cmdRegex, RecordingSshTool.CustomResponseGenerator response) {
            customResponses.put(cmdRegex, checkNotNull(response, "response"));
        }

        public static void setCustomResponse(String cmdRegex, RecordingSshTool.CustomResponse response) {
            customResponses.put(cmdRegex, checkNotNull(response, "response").toGenerator());
        }
    }

    protected SshMachineLocation newHost() {
        LocalhostMachineProvisioningLocation localhostMachineProvisioningLocation = (LocalhostMachineProvisioningLocation) mgmt.getLocationRegistry().getLocationManaged("localhost");
        localhostMachineProvisioningLocation.config().putAll(ImmutableMap.of(BROOKLYN_CONFIG_KEY_PREFIX + PROP_EXEC_ASYNC.getName(), true));
        localhostMachineProvisioningLocation.config().set(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshjTool.class.getName());
        try {
            return localhostMachineProvisioningLocation.obtain();
        } catch (NoMachinesAvailableException e) {
            throw Exceptions.propagate(e);
        }
    }

    @BeforeClass(alwaysRun = true)
    public void enableSshjExecAsync() {
        BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_SSH_ASYNC_EXEC);
    }

    @AfterClass(alwaysRun = true)
    public void disableSshjExecAsync() {
        BrooklynFeatureEnablement.disable(BrooklynFeatureEnablement.FEATURE_SSH_ASYNC_EXEC);
    }

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        host = newHost();
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        Assert.assertEquals(RecordingSshjTool.commandsExecuted.size(), 1);
        RecordingSshjTool.commandsExecuted.clear();
        super.tearDown();
    }

    @Test
    public void testSshExecCommands() throws Exception {
        String expectedName = Os.user();
        RecordingSshjTool.setCustomResponse(".*whoami.*", new RecordingSshTool.CustomResponse(0, expectedName, ""));

        OutputStream outStream = new ByteArrayOutputStream();
        host.execCommands(MutableMap.of("out", outStream), "mysummary", ImmutableList.of("whoami; exit"));
        String outString = outStream.toString();

        assertTrue(outString.contains(expectedName), outString);
    }
}
