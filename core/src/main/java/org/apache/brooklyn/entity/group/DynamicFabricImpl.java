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
package org.apache.brooklyn.entity.group;

import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Changeable;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.enricher.stock.Enrichers;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * When a dynamic fabric is started, it starts an entity in each of its locations. 
 * This entity will be the parent of each of the started entities. 
 */
public class DynamicFabricImpl extends AbstractGroupImpl implements DynamicFabric {
    private static final Logger logger = LoggerFactory.getLogger(DynamicFabricImpl.class);

    public DynamicFabricImpl() {
    }

    @Override
    public void init() {
        super.init();
        
        enrichers().add(Enrichers.builder()
                .aggregating(Changeable.GROUP_SIZE)
                .publishing(FABRIC_SIZE)
                .fromMembers()
                .computingSum()
                .valueToReportIfNoSensors(0)
                .build());
        
        sensors().set(SERVICE_UP, false);
    }
    
    protected EntitySpec<?> getFirstMemberSpec() {
        return getConfig(FIRST_MEMBER_SPEC);
    }

    protected EntitySpec<?> getMemberSpec() {
        return getConfig(MEMBER_SPEC);
    }
    
    protected String getDisplayNamePrefix() {
        return getConfig(DISPLAY_NAME_PREFIX);
    }
    
    protected String getDisplayNameSuffix() {
        return getConfig(DISPLAY_NAME_SUFFIX);
    }
    
    @Override
    public void setMemberSpec(EntitySpec<?> memberSpec) {
        setConfigEvenIfOwned(MEMBER_SPEC, memberSpec);
    }
    
    @Override
    public void start(Collection<? extends Location> locsO) {
        boolean includeInitialChildren = Boolean.TRUE.equals(config().get(INCLUDE_INITIAL_CHILDREN));
        
        addLocations(locsO);
        Collection<? extends Location> allLocations = Locations.getLocationsCheckingAncestors(locsO, this);
        
        Preconditions.checkNotNull(allLocations, "locations must be supplied");
        Preconditions.checkArgument(allLocations.size() >= 1, "One or more locations must be supplied");
        
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        try {
            Map<Entity, Task<?>> tasks = Maps.newLinkedHashMap();
            List<Location> locationsForMembers = startChildren(includeInitialChildren, allLocations, tasks);

            // finally (and usually) we create new entities for locations passed in
            // (unless they were consumed by pre-existing children which didn't have locations)
            for (Location it : locationsForMembers) {
                Entity e = addCluster(it);
                
                ((EntityInternal)e).addLocations(Arrays.asList(it));
                if (e instanceof Startable) {
                    Task<?> task = Entities.submit(this,
                        Effectors.invocation(e, START, ImmutableMap.of("locations", ImmutableList.of(it))).asTask());
                    tasks.put(e, task);
                }
            }
            
            waitForTasksOnStart(tasks);
            ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
            sensors().set(SERVICE_UP, true);
        } catch (Exception e) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(e);
        }
    }

    /**
     * If including initial children, first look at existing Startable children - start them with 
     * the locations passed in here (if they have no locations yet). This "consumes" a location
     * so we won't create an additional member in that location.
     * 
     * If not {@code includeInitialChildren}, then start those children in the first location but
     * don't "consume" a location - so we'll create additional members.
     * 
     * @param includeInitialChildren
     * @param allLocations
     * @param tasks side-effects this map, to add the tasks created for starting the child entities
     * 
     * @return unused locations
     */
    protected List<Location> startChildren(boolean includeInitialChildren, Collection<? extends Location> allLocations, Map<Entity, Task<?>> tasks) {
        List<Location> locations = MutableList.copyOf(allLocations);
        int locIndex = 0;
        
        for (Entity child: getChildren()) {
            if (child instanceof Startable) {
                if (includeInitialChildren) {
                    addMember(child);
                }
                
                Location it = null;
                if (child.getLocations().isEmpty()) {
                    // give him any of these locations if he has none, allowing round robin here
                    if (!locations.isEmpty()) {
                        if (includeInitialChildren) {
                            it = locations.get(locIndex++ % locations.size());
                        } else {
                            it = locations.get(0);
                        }
                        ((EntityInternal)child).addLocations(Arrays.asList(it));
                    }
                }
                
                tasks.put(child, Entities.submit(this,
                    Effectors.invocation(child, START, ImmutableMap.of("locations", 
                        it==null ? ImmutableList.of() : ImmutableList.of(it))).asTask()));
            }
        }
            
        // remove all the locations we applied to existing nodes
        while (locIndex-->0 && !locations.isEmpty()) {
            locations.remove(0);
        }
        
        return locations;
    }
    
    protected void waitForTasksOnStart(Map<Entity, Task<?>> tasks) {
        // TODO Could do best-effort for waiting for remaining tasks, rather than failing on first?

        for (Map.Entry<Entity, Task<?>> entry: tasks.entrySet()) {
            try {
                entry.getValue().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            } catch (ExecutionException ee) {
                throw Throwables.propagate(ee.getCause());
            }
        }
    }
    
    @Override
    public void stop() {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPING);
        try {
            Iterable<Entity> stoppableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(Startable.class));
            Task<?> invoke = Entities.invokeEffector(this, stoppableChildren, Startable.STOP);
            if (invoke != null) invoke.get();
            ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPED);
            sensors().set(SERVICE_UP, false);
        } catch (Exception e) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public void restart() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getFabricSize() {
        int result = 0;
        for (Entity child : getChildren()) {
            result  += elvis(child.getAttribute(Changeable.GROUP_SIZE), 0);
        }
        return result;
    }
    
    @Override
    public boolean removeChild(Entity child) {
        boolean changed = super.removeChild(child);
        if (changed) {
            removeMember(child);
        }
        return changed;
    }
    
    protected Map getCustomChildFlags() {
        Map result = getConfig(CUSTOM_CHILD_FLAGS);
        return (result == null) ? ImmutableMap.of() : result;
    }
    
    protected Entity addCluster(Location location) {
        String locationName = elvis(location.getDisplayName(), location.getDisplayName(), null);
        Map creation = Maps.newLinkedHashMap();
        creation.putAll(getCustomChildFlags());
        if (groovyTruth(getDisplayNamePrefix()) || groovyTruth(getDisplayNameSuffix())) {
            String displayName = "" + elvis(getDisplayNamePrefix(), "") + elvis(locationName, "unnamed") + elvis(getDisplayNameSuffix(),"");
            creation.put("displayName", displayName);
        }
        logger.info("Creating entity in fabric {} at {}{}", new Object[] {this, location, 
                (creation!=null && !creation.isEmpty() ? ", properties "+creation : "") });

        Entity entity = createCluster(location, creation);
        
        if (locationName != null) {
            if (entity.getDisplayName()==null)
                entity.setDisplayName(entity.getEntityType().getSimpleName() +" ("+locationName+")");
            else if (!entity.getDisplayName().contains(locationName)) 
                entity.setDisplayName(entity.getDisplayName() +" ("+locationName+")");
        }
        if (entity.getParent()==null) entity.setParent(this);
        
        // Continue to call manage(), because some uses of NodeFactory (in tests) still instantiate the
        // entity via its constructor
        Entities.manage(entity);
        
        addMember(entity);
        
        return entity;
    }
    
    protected Entity createCluster(Location location, Map flags) {
        EntitySpec<?> memberSpec = null;
        if (getMembers().isEmpty()) memberSpec = getFirstMemberSpec();
        if (memberSpec == null) memberSpec = getMemberSpec();

        if (memberSpec == null) {
            throw new IllegalStateException("No member spec nor entity factory supplied for dynamic fabric "+this);
        }
        EntitySpec<?> specConfigured = EntitySpec.create(memberSpec).configure(flags);
        if (location!=null) specConfigured.location(location);
        return addChild(specConfigured);
    }
}
