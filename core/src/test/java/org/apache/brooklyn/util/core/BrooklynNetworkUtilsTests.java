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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

public class BrooklynNetworkUtilsTests {
    @Test
    public void testPortRulesToRanges() throws Exception {
        RangeSet<Integer> actualRangeSet = BrooklynNetworkUtils.portRulesToRanges(ImmutableList.of(
                "22", "23", "5000-6000", "8081", "80-90", "90-100", "23",
                "8081"));

        Asserts.assertEquals(actualRangeSet, ImmutableRangeSet.<Integer>builder()
                .add(Range.closed(22, 22))
                .add(Range.closed(23, 23))
                .add(Range.closed(80, 100))
                .add(Range.closed(5000, 6000))
                .add(Range.closed(8081, 8081))
                .build());
    }
}
