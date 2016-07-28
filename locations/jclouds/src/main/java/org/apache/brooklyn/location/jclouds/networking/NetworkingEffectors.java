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
package org.apache.brooklyn.location.jclouds.networking;

import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.BrooklynNetworkUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.net.Cidr;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;

import java.util.List;

public class NetworkingEffectors {
    // Intentionally not use CloudLocationConfig.INBOUND_PORTS to make richer syntax and rename it to differ it from the first in a ConfigBag
    public static final ConfigKey<List<String>> INBOUND_PORTS_LIST = ConfigKeys.newConfigKey(new TypeToken<List<String>>() {}, "inbound.ports.list",
            "Ports to open from the effector", ImmutableList.<String>of());
    public static final ConfigKey<IpProtocol> INBOUND_PORTS_LIST_PROTOCOL = ConfigKeys.newConfigKey(new TypeToken<IpProtocol>() {}, "inbound.ports.list.protocol",
            "Protocol for ports to open. Possible values: TCP, UDP, ICMP, ALL.", IpProtocol.TCP);

    public static Effector<Void> openPortsInSecurityGroupEffector(Function<EffectorBody<Void>, Void> portOpener) {
        return Effectors.effector(Void.class, "openPortsInSecurityGroup")
                .parameter(INBOUND_PORTS_LIST)
                .parameter(INBOUND_PORTS_LIST_PROTOCOL)
                .description("Open ports in Cloud Security Group. If called before machine location is provisioned, it will fail.")
                .impl(new OpenPortsInSecurityGroupBody(portOpener))
                .build();
    }

    public static class OpenPortsInSecurityGroupBody<T> extends EffectorBody<T> {
        private Function<EffectorBody<T>, T> portOpener;
        public OpenPortsInSecurityGroupBody(Function<EffectorBody<T>, T> portOpener) {
            Preconditions.checkNotNull(portOpener);
            this.portOpener = portOpener;
        }
        @Override
        public T call(ConfigBag parameters) {
            List<String> rawPortRules = parameters.get(INBOUND_PORTS_LIST);
            IpProtocol ipProtocol = parameters.get(INBOUND_PORTS_LIST_PROTOCOL);
            Preconditions.checkNotNull(ipProtocol);
            Preconditions.checkNotNull(rawPortRules, "ports cannot be null");
            MutableList.Builder<IpPermission> ipPermissionsBuilder = MutableList.builder();
            for (Range<Integer> portRule : BrooklynNetworkUtils.portRulesToRanges(rawPortRules).asRanges()) {
                ipPermissionsBuilder.add(
                        IpPermission.builder()
                                .ipProtocol(ipProtocol)
                                .fromPort(portRule.lowerEndpoint())
                                .toPort(portRule.upperEndpoint())
                                .cidrBlock(Cidr.UNIVERSAL.toString())
                                .build());
            }
            return portOpener.apply(this);
        }

        public static class JcloudsSecurityGroupPortOpener implements Function<EffectorBody<Void>, Void> {
            private Iterable<IpPermission> ipPermissions;

            public JcloudsSecurityGroupPortOpener(Iterable<IpPermission> ipPermissions) {
                this.ipPermissions = ipPermissions;
            }

            @Override
            public Void apply(EffectorBody<Void> effectorBody) {
                Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
                JcloudsLocationSecurityGroupCustomizer customizer = JcloudsLocationSecurityGroupCustomizer.getInstance(entity);

                Optional<Location> jcloudsMachineLocation = Iterables.tryFind(entity.getLocations(), Predicates.instanceOf(JcloudsMachineLocation.class));
                if (!jcloudsMachineLocation.isPresent()) {
                    throw new IllegalArgumentException("Tried to execute open ports effector on an entity with no JcloudsMachineLocation");
                }
                customizer.addPermissionsToLocation((JcloudsMachineLocation)jcloudsMachineLocation.get(), ipPermissions);
                return null;
            }
        }
    }
}
