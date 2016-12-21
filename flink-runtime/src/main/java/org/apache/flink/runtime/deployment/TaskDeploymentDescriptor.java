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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 */
public final class TaskDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -3233562176034358530L;

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(TaskDeploymentDescriptor.class);

	/**
	 * Maximum size of the serialized job and task information fields inside
	 * {@link #data} before they will be offloaded to the blob server instead
	 * of serializing them with the {@link TaskDeploymentDescriptor} object and
	 * sending them inside RPCs directly (use 1 KiB for now).
	 */
	public static final int MAX_SHORT_MESSAGE_SIZE = 1 * 1024;

	private static class TaskDeploymentDescriptorData implements Serializable {
		private static final long serialVersionUID = -6762050730992879625L;

		/**
		 * Serialized job information
		 */
		final SerializedValue<JobInformation> serializedJobInformation;

		/**
		 * Serialized task information
		 */
		final SerializedValue<TaskInformation> serializedTaskInformation;

		TaskDeploymentDescriptorData(
			SerializedValue<JobInformation> serializedJobInformation,
			SerializedValue<TaskInformation> serializedTaskInformation) {

			this.serializedJobInformation =
				Preconditions.checkNotNull(serializedJobInformation);
			this.serializedTaskInformation =
				Preconditions.checkNotNull(serializedTaskInformation);
		}
	}

	/** Potentially big data which may be externalized. */
	private TaskDeploymentDescriptorData data;

	/**
	 * The ID referencing the job this task belongs to.
	 *
	 * <p>NOTE: this is redundant to the information stored in {@link
	 * TaskDeploymentDescriptorData#serializedJobInformation} but needed in
	 * order to restore offloaded data.</p>
	 */
	private final JobID jobId;

	/** The ID referencing the attempt to execute the task. */
	private final ExecutionAttemptID executionId;

	/** The allocation ID of the slot in which the task shall be run */
	private final AllocationID allocationId;

	/** The task's index in the subtask group. */
	private final int subtaskIndex;

	/** Attempt number the task */
	private final int attemptNumber;

	/** The list of produced intermediate result partition deployment descriptors. */
	private final Collection<ResultPartitionDeploymentDescriptor> producedPartitions;

	/** The list of consumed intermediate result partitions. */
	private final Collection<InputGateDeploymentDescriptor> inputGates;

	/** Slot number to run the sub task in on the target machine */
	private final int targetSlotNumber;

	/** State handles for the sub task */
	private final TaskStateHandles taskStateHandles;

	public TaskDeploymentDescriptor(
			JobID jobId,
			SerializedValue<JobInformation> serializedJobInformation,
			SerializedValue<TaskInformation> serializedTaskInformation,
			ExecutionAttemptID executionAttemptId,
			AllocationID allocationId,
			int subtaskIndex,
			int attemptNumber,
			int targetSlotNumber,
			TaskStateHandles taskStateHandles,
			Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

		this.jobId = jobId;
		this.data = new TaskDeploymentDescriptorData(serializedJobInformation,
			serializedTaskInformation);
		this.executionId = Preconditions.checkNotNull(executionAttemptId);
		this.allocationId = Preconditions.checkNotNull(allocationId);

		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		this.subtaskIndex = subtaskIndex;

		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		this.attemptNumber = attemptNumber;

		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");
		this.targetSlotNumber = targetSlotNumber;

		this.taskStateHandles = taskStateHandles;

		this.producedPartitions = Preconditions.checkNotNull(resultPartitionDeploymentDescriptors);
		this.inputGates = Preconditions.checkNotNull(inputGateDeploymentDescriptors);
	}

	/**
	 * Return the sub task's serialized job information.
	 *
	 * @return serialized job information
	 */
	public SerializedValue<JobInformation> getSerializedJobInformation() {
		return data.serializedJobInformation;
	}

	/**
	 * Return the sub task's serialized task information.
	 *
	 * @return serialized task information
	 */
	public SerializedValue<TaskInformation> getSerializedTaskInformation() {
		return data.serializedTaskInformation;
	}

	public ExecutionAttemptID getExecutionAttemptId() {
		return executionId;
	}

	/**
	 * Returns the task's index in the subtask group.
	 *
	 * @return the task's index in the subtask group
	 */
	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	/**
	 * Returns the attempt number of the subtask
	 */
	public int getAttemptNumber() {
		return attemptNumber;
	}

	/**
	 * Gets the number of the slot into which the task is to be deployed.
	 *
	 * @return The number of the target slot.
	 */
	public int getTargetSlotNumber() {
		return targetSlotNumber;
	}

	public Collection<ResultPartitionDeploymentDescriptor> getProducedPartitions() {
		return producedPartitions;
	}

	public Collection<InputGateDeploymentDescriptor> getInputGates() {
		return inputGates;
	}

	public TaskStateHandles getTaskStateHandles() {
		return taskStateHandles;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	/**
 	 * Tries to store big data from {@link TaskDeploymentDescriptorData#serializedJobInformation}
 	 * and {@link TaskDeploymentDescriptorData#serializedTaskInformation} in the given
 	 * <tt>blobServer</tt> instead of sending it in an RPC.
 	 *
 	 * @param blobServer     the blob server to use
	 * @return whether the data has been offloaded or not
 	 */
	public boolean tryOffLoadBigData(final BlobServer blobServer) {
		// more than MAX_SHORT_MESSAGE_SIZE?
		if (data.serializedJobInformation.getByteArray().length +
			data.serializedTaskInformation.getByteArray().length > MAX_SHORT_MESSAGE_SIZE) {

			// write out data and re-set the data object
			try {
				final String fileKey = getOffloadedFileName();
				blobServer.putObject(data, jobId, fileKey);
				data = null;
				return true;
			} catch (IOException e) {
				LOG.warn("Failed to offload data to BLOB store", e);
			}
		}
		return false;
	}

	/**
	 * Loads externalized data from {@link #tryOffLoadBigData(BlobServer)} back to the
	 * object.
	 *
	 * @param blobService     the blob store to use
	 *
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * 		Class of a serialized object cannot be found.
	 */
	public void loadBigData(final BlobService blobService)
			throws IOException, ClassNotFoundException {
		if (data == null) {
			final String fileKey = getOffloadedFileName();
			final String dataFile = blobService.getURL(jobId, fileKey).getFile();
			final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(dataFile));
			data = (TaskDeploymentDescriptorData) ois.readObject();
			// delete from local cache
			// TODO: keep around for restarts?
			blobService.delete(jobId, fileKey);
		}
	}

	private final String getOffloadedFileName() {
		return String.format("TaskDeploymentDescriptor-%s.dat",
			executionId.toString());
	}

	@Override
	public String toString() {
		return String.format("TaskDeploymentDescriptor [execution id: %s, attempt: %d, " +
				"produced partitions: %s, input gates: %s]",
			executionId,
			attemptNumber,
			collectionToString(producedPartitions),
			collectionToString(inputGates));
	}

	private static String collectionToString(Iterable<?> collection) {
		final StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");

		for (Object elem : collection) {
			strBuilder.append(elem);
		}

		strBuilder.append("]");

		return strBuilder.toString();
	}
}
