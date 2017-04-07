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
package org.apache.brooklyn.core.mgmt.persist;

import com.google.common.base.Function;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

@Test
public class BrooklynMementoPersisterTransformersInMemoryTest extends BrooklynMementoPersisterTestFixture {

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        RecordingPersisterPutTransformer.putCounter.set(0);
        RecordingPersisterGetTransformer.getCounter.set(0);
        super.setUp();
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        super.tearDown();
        Assert.assertTrue(RecordingPersisterPutTransformer.putCounter.get() > 0, "Should be grater than zero.");
        Assert.assertTrue(RecordingPersisterGetTransformer.getCounter.get() > 0, "Should be grater than zero.");
    }

    protected ManagementContext newPersistingManagementContext() {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put("persister.put.transformer",RecordingPersisterPutTransformer.class.getName());
        brooklynProperties.put("persister.get.transformer",RecordingPersisterGetTransformer.class.getName());
        return RebindTestUtils.managementContextBuilder(classLoader, new InMemoryObjectStore()).properties(brooklynProperties)
                .persistPeriod(Duration.millis(10)).buildStarted();
    }

    public static class RecordingPersisterPutTransformer implements Function<String, String> {
        static final String PUT_PREFIX = "[PUT_TRANSFORMER_PREFIX]";
        static final AtomicInteger putCounter = new AtomicInteger();

        @Nullable
        @Override
        public String apply(@Nullable String input) {
            putCounter.getAndIncrement();
            return PUT_PREFIX + input;
        }
    }

    public static class RecordingPersisterGetTransformer implements Function<String, String> {
        static final AtomicInteger getCounter = new AtomicInteger();
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            getCounter.getAndIncrement();
            return input.substring(RecordingPersisterPutTransformer.PUT_PREFIX.length());
        }
    }
}
