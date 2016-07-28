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
package org.apache.brooklyn.entity.software.base.location;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.location.jclouds.networking.JcloudsLocationSecurityGroupCustomizer;
import org.apache.brooklyn.location.jclouds.networking.NetworkingEffectors;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.util.IpPermissions;
import org.testng.annotations.Test;

import java.util.List;

public class NetworkingEffectorsTests extends BrooklynAppUnitTestSupport {
    @Test
    public void testPassSecurityGroupParameters() {
        EmptySoftwareProcess emptySoftwareProcess = app.addChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(SoftwareProcess.ADD_OPEN_PORTS_EFFECTOR, true));

        Iterable<Object> permissions = ImmutableList.<Object>of("234", "324");
//        emptySoftwareProcess.invoke(
//                NetworkingEffectors.openPortsInSecurityGroupEffector(emptySoftwareProcess),
//                ImmutableMap.<String, Object>of(
//                        NetworkingEffectors.INBOUND_PORTS_LIST.getName(), ""
//                        ));
    }

    private class MockedJcloudsLocationSecurityGroupCustomizer extends JcloudsLocationSecurityGroupCustomizer {
        private List<IpPermission> expectedValues;
        protected MockedJcloudsLocationSecurityGroupCustomizer(String applicationId, List<IpPermission> expectedValues) {
            super(applicationId);
            this.expectedValues = expectedValues;
        }

        @Override
        public JcloudsLocationSecurityGroupCustomizer addPermissionsToLocation(final JcloudsMachineLocation location, final Iterable<IpPermission> permissions) {
            return null;
        }
    }
}
