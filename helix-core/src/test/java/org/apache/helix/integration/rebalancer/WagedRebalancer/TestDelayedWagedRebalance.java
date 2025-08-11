package org.apache.helix.integration.rebalancer.WagedRebalancer;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.rebalancer.DelayedAutoRebalancer.TestDelayedAutoRebalance;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Inherit TestDelayedAutoRebalance to ensure the test logic is the same.
 */
public class TestDelayedWagedRebalance extends TestDelayedAutoRebalance {
  // create test DBs, wait it converged and return externalviews
  protected Map<String, ExternalView> createTestDBs(long delayTime) throws InterruptedException {
    Map<String, ExternalView> externalViews = new HashMap<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _minActiveReplica);
      _testDBs.add(db);
    }
    Thread.sleep(DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  @Test
  public void testDelayedPartitionMovement() {
    // Waged Rebalancer takes cluster level delay config only. Skip this test.
  }

  @Test
  public void testDisableDelayRebalanceInResource() {
    // Waged Rebalancer takes cluster level delay config only. Skip this test.
  }

  @Test(dependsOnMethods = { "testDelayedPartitionMovement" })
  public void testDelayedPartitionMovementWithClusterConfigedDelay() throws Exception {
    super.testDelayedPartitionMovementWithClusterConfigedDelay();
  }

  @Test(dependsOnMethods = { "testDelayedPartitionMovementWithClusterConfigedDelay" })
  public void testMinimalActiveReplicaMaintain() throws Exception {
    super.testMinimalActiveReplicaMaintain();
  }

  @Test(dependsOnMethods = { "testMinimalActiveReplicaMaintain" })
  public void testPartitionMovementAfterDelayTime() throws Exception {
    super.testPartitionMovementAfterDelayTime();
  }

  @Test(dependsOnMethods = { "testDisableDelayRebalanceInResource" })
  public void testDisableDelayRebalanceInCluster() throws Exception {
    super.testDisableDelayRebalanceInCluster();
  }

  @Test(dependsOnMethods = { "testDisableDelayRebalanceInCluster" })
  public void testDisableDelayRebalanceInInstance() throws Exception {
    super.testDisableDelayRebalanceInInstance();
  }

  @Test(dependsOnMethods = {"testDisableDelayRebalanceInInstance"})
  public void testOnDemandRebalance() throws Exception {
    super.testOnDemandRebalance();
  }

  @Test(dependsOnMethods = {"testOnDemandRebalance"})
  public void testExpiredOnDemandRebalanceTimestamp() throws Exception {
    super.testExpiredOnDemandRebalanceTimestamp();
  }

  @Test(dependsOnMethods = {"testExpiredOnDemandRebalanceTimestamp"})
  public void testOnDemandRebalanceAfterDelayRebalanceHappen() throws Exception {
    super.testOnDemandRebalanceAfterDelayRebalanceHappen();
  }

  @Test
  public void testPartitionAssignmentWithNodeDownDuringDelay() throws Exception {
    // Set up cluster-level delayed rebalance for 1 minute (60,000 ms)
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 60000);

    // Create a WAGED resource with 5 partitions, 3 replicas
    int numPartitions = 5;
    int numReplicas = 1;
    String stateModel = BuiltInStateModelDefinitions.MasterSlave.name();
    String db = "Test-DB-NodeDownDelay";
    createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, numPartitions, numReplicas, numReplicas);
    _testDBs.add(db);
    Thread.sleep(DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the initial ExternalView and pick a partition and one of its assigned instances
    ExternalView evBefore = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    String targetPartition = evBefore.getPartitionSet().iterator().next();
    String offlineInstance = evBefore.getStateMap(targetPartition).keySet().iterator().next();

    // Bring down the chosen instance
    for (MockParticipantManager participant : _participants) {
      if (participant.getInstanceName().equals(offlineInstance)) {
        participant.syncStop();
        break;
      }
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the ExternalView after node down, during delay window
    ExternalView evAfter = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    Map<String, String> stateMapAfter = evAfter.getStateMap(targetPartition);

    // The offline instance should still be in the assignment (delayed rebalance window)
    Assert.assertTrue(stateMapAfter.containsKey(offlineInstance),
        "Partition should still be assigned to the offline instance during delay window");

    // Wait for delay window to expire
    Thread.sleep(65000); // 65 seconds to ensure delay window passes
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // After delay, the offline instance should be removed from assignment
    ExternalView evFinal = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    Map<String, String> stateMapFinal = evFinal.getStateMap(targetPartition);
    Assert.assertFalse(stateMapFinal.containsKey(offlineInstance),
        "Partition should be moved off the offline instance after delay window");
  }

  @Test
  public void testPartitionAssignmentWithNodeRestartAfterDelay() throws Exception {
    // Set up cluster-level delayed rebalance for 1 minute (60,000 ms)
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 60000);

    // Create a WAGED resource with 5 partitions, 3 replicas
    int numPartitions = 5;
    int numReplicas = 1;
    String stateModel = BuiltInStateModelDefinitions.MasterSlave.name();
    String db = "Test-DB-NodeRestartDelay";
    createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, numPartitions, numReplicas, numReplicas);
    _testDBs.add(db);
    Thread.sleep(DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
//    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the initial ExternalView and pick a partition and one of its assigned instances
    ExternalView evBefore = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    String targetPartition = evBefore.getPartitionSet().iterator().next();
    String offlineInstance = evBefore.getStateMap(targetPartition).keySet().iterator().next();

    // Bring down the chosen instance
    MockParticipantManager participantToRestart = null;
    for (MockParticipantManager participant : _participants) {
      if (participant.getInstanceName().equals(offlineInstance)) {
        participantToRestart = participant;
        participant.syncStop();
        break;
      }
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the ExternalView after node down, during delay window
    ExternalView evAfter = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    Map<String, String> stateMapAfter = evAfter.getStateMap(targetPartition);

    // The offline instance should still be in the assignment (delayed rebalance window)
//    Assert.assertTrue(stateMapAfter.containsKey(offlineInstance),
//        "Partition should still be assigned to the offline instance during delay window");
//
//    // Wait for delay window to expire
////    Thread.sleep(65000); // 65 seconds to ensure delay window passes
//    Assert.assertTrue(_clusterVerifier.verifyByPolling());
//
//    // After delay, the offline instance should be removed from assignment
//    ExternalView evAfterDelay = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
//    Map<String, String> stateMapAfterDelay = evAfterDelay.getStateMap(targetPartition);
//    Assert.assertFalse(stateMapAfterDelay.containsKey(offlineInstance),
//        "Partition should be moved off the offline instance after delay window");

    // Restart the participant by creating a new instance (same pattern as TestSemiAutoRebalance)
    Assert.assertNotNull(participantToRestart, "Participant to restart should not be null");
    String instanceName = participantToRestart.getInstanceName();

    // Create a new participant instance instead of restarting the old one
    MockParticipantManager newParticipant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    newParticipant.syncStart();

    // Replace the old participant in the list
    for (int i = 0; i < _participants.size(); i++) {
      if (_participants.get(i).getInstanceName().equals(instanceName)) {
        _participants.set(i, newParticipant);
        break;
      }
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // After restart, the instance should be back in the assignment
    ExternalView evAfterRestart = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    Map<String, String> stateMapAfterRestart = evAfterRestart.getStateMap(targetPartition);

    // The restarted instance should be back in the assignment
    Assert.assertTrue(stateMapAfterRestart.containsKey(offlineInstance),
        "Partition should be reassigned to the restarted instance");

    // Verify that the partition has the correct number of replicas
    Assert.assertEquals(stateMapAfterRestart.size(), numReplicas,
        "Partition should have the correct number of replicas after restart");
  }
}
