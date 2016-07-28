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
package org.apache.brooklyn.util.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.brooklyn.core.location.geo.LocalhostExternalIpLoader;
import org.apache.brooklyn.core.server.BrooklynServiceAttributes;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.net.Networking;

import java.net.InetAddress;
import java.util.Collection;

public class BrooklynNetworkUtils {

    /** returns the externally-facing IP address from which this host comes, or 127.0.0.1 if not resolvable */
    public static String getLocalhostExternalIp() {
        return LocalhostExternalIpLoader.getLocalhostIpQuicklyOrDefault();
    }

    /** returns a IP address for localhost paying attention to a system property to prevent lookup in some cases */ 
    public static InetAddress getLocalhostInetAddress() {
        return TypeCoercions.coerce(JavaGroovyEquivalents.elvis(BrooklynServiceAttributes.LOCALHOST_IP_ADDRESS.getValue(), 
                Networking.getLocalHost()), InetAddress.class);
    }

    // TODO it does not add adjacent intervals: {[22, 22], [23, 23]} is not merged to {[22, 23]}
    public static RangeSet<Integer> portRulesToRanges(Collection<String> portRules) {
        RangeSet<Integer> result = TreeRangeSet.create();
        for (String portRule : portRules) {
            if (portRule.contains("-")) {
                String[] fromTo = portRule.split("-");
                Preconditions.checkState(fromTo.length == 2);
                result.add(closedRange(fromTo[0], fromTo[1]));
            } else {
                result.add(closedRange(portRule, portRule));
            }
        }
        return result;
    }

    private static Range<Integer> closedRange(String from, String to) {
        return Range.closed(Integer.parseInt(from), Integer.parseInt(to));
    }
}
