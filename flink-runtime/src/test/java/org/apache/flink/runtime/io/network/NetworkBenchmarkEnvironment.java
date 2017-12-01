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

package org.apache.flink.runtime.io.network;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup.DummyTaskIOMetricGroup;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.types.LongValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.netty.NettyConfig.NUM_THREADS_CLIENT;
import static org.apache.flink.runtime.io.network.netty.NettyConfig.NUM_THREADS_SERVER;
import static org.apache.flink.util.ExceptionUtils.suppressExceptions;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Context for network benchmarks executed in flink-benchmark.
 */
public class NetworkBenchmarkEnvironment<T extends IOReadableWritable> {

	private static final int BUFFER_SIZE = TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue();

	/**
	 * Number of netty IO threads per sender and receiver.
	 */
	private static final int NUM_IO_THREADS = 8;

	private static final InetAddress LOCAL_ADDRESS;

	static {
		try {
			LOCAL_ADDRESS = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new Error(e);
		}
	}

	protected final JobID jobId = new JobID();
	protected final IntermediateDataSetID dataSetID = new IntermediateDataSetID();
	protected final ResultPartitionID senderID = new ResultPartitionID();
	protected final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

	protected NetworkEnvironment senderEnv;
	protected NetworkEnvironment receiverEnv;
	protected IOManager ioManager;

	public void setUp() throws Exception {
		senderEnv = createNettyNetworkEnvironment(2048);
		receiverEnv = createNettyNetworkEnvironment(2048);
		ioManager = new IOManagerAsync();

		senderEnv.start();
		receiverEnv.start();
	}

	public void tearDown() {
		suppressExceptions(senderEnv::shutdown);
		suppressExceptions(receiverEnv::shutdown);
		suppressExceptions(ioManager::shutdown);
	}

	public RecordWriter<T> createRecordWriter() throws Exception {
		ResultPartitionWriter sender = createResultPartition(jobId, senderID, senderEnv);
		return new RecordWriter<>(sender);
	}

	public <C extends Receiver> C createReceiver(Class<C> clazz) throws Exception {
		TaskManagerLocation senderLocation = new TaskManagerLocation(
			ResourceID.generate(),
			LOCAL_ADDRESS,
			senderEnv.getConnectionManager().getDataPort());

		SingleInputGate receiverGate = createInputGate(
			jobId, dataSetID, senderID, executionAttemptID, senderLocation, receiverEnv);

		final C receiver;
		// avoid reflection or duplicates (as enum values) here
		if (clazz == SerializingLongReceiver.class) {
			receiver = (C) new SerializingLongReceiver(receiverGate);
		} else if (clazz == DroppingNonDeserializingLongReceiver.class) {
			receiver = (C) new DroppingNonDeserializingLongReceiver(receiverGate);
		} else {
			throw new UnsupportedOperationException("No receiver class " + clazz);
		}

		receiver.start();
		return receiver;
	}

	// ------------------------------------------------------------------------
	//  Receivers
	// ------------------------------------------------------------------------


	public static abstract class Receiver extends CheckedThread {
		protected static final Logger LOG = LoggerFactory.getLogger(Receiver.class);

		protected CompletableFuture<Long> expectedRecords = new CompletableFuture<>();
		protected CompletableFuture<?> recordsProcessed = new CompletableFuture<>();

		protected volatile boolean running;

		Receiver() {
			setName(this.getClass().getName());

			this.running = true;
		}

		public synchronized CompletableFuture<?> setExpectedRecords(long nextExpectedRecordsBatch) {
			checkState(!expectedRecords.isDone());
			checkState(!recordsProcessed.isDone());

			expectedRecords.complete(nextExpectedRecordsBatch);
			return recordsProcessed;
		}

		protected synchronized CompletableFuture<Long> getExpectedRecords() {
			return expectedRecords;
		}

		protected synchronized void finishProcessingExpectedRecords() {
			checkState(expectedRecords.isDone());
			checkState(!recordsProcessed.isDone());

			recordsProcessed.complete(null);
			expectedRecords = new CompletableFuture<>();
			recordsProcessed = new CompletableFuture<>();
		}

		@Override
		public void go() throws Exception {
			try {
				while (running) {
					readRecords(getExpectedRecords().get());
					finishProcessingExpectedRecords();
				}
			}
			catch (InterruptedException e) {
				if (running) {
					throw e;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		protected abstract void readRecords(long remaining) throws Exception;

		public void shutdown() {
			running = false;
			interrupt();
			expectedRecords.complete(0L);
		}

	}

	public static class SerializingLongReceiver extends Receiver {

		private long maxLatency = Long.MIN_VALUE;
		private long minLatency = Long.MAX_VALUE;
		private long sumLatency;
		private long sumLatencySquare;
		private int numSamples;

		private final MutableRecordReader<LongValue> reader;

		public SerializingLongReceiver(InputGate inputGate) {
			super();
			this.reader = new MutableRecordReader<>(
				inputGate,
				new String[] {
					EnvironmentInformation.getTemporaryFileDirectory()
				});
		}

		protected void readRecords(long remaining) throws Exception {
			LOG.debug("readRecords(remaining = {})", remaining);
			final LongValue value = new LongValue();

			while (running && remaining-- > 0 && reader.next(value)) {
				final long ts = value.getValue();
				if (ts != 0) {
					final long latencyNanos = System.nanoTime() - ts;

					maxLatency = Math.max(maxLatency, latencyNanos);
					minLatency = Math.min(minLatency, latencyNanos);
					sumLatency += latencyNanos;
					sumLatencySquare += latencyNanos * latencyNanos;
					numSamples++;
				}
			}

		}

		public long getMaxLatency() {
			return maxLatency == Long.MIN_VALUE ? 0 : maxLatency;
		}

		public long getMinLatency() {
			return minLatency == Long.MAX_VALUE ? 0 : minLatency;
		}

		public long getAvgLatency() {
			return numSamples == 0 ? 0 : sumLatency / numSamples;
		}

		public long getAvgLatencyNoExtremes() {
			return (numSamples > 2) ? (sumLatency - maxLatency - minLatency) / (numSamples - 2) : 0;
		}

		public long getStandardDeviationLatency() {
			return numSamples == 0 ? 0 : (long) Math.sqrt(
				sumLatencySquare / numSamples - sumLatency * sumLatency /
					(numSamples * numSamples));
		}

		@Override
		public String toString() {
			return String.format("Receiver[avg=%d (%d), min=%d, max=%d, stddev=%d]",
				getAvgLatency(), getAvgLatencyNoExtremes(), getMinLatency(), getMaxLatency(),
				getStandardDeviationLatency());
		}
	}

	@SuppressWarnings("unused")
	public static class DroppingNonDeserializingLongReceiver extends Receiver {

		private final InputGate inputGate;

		public DroppingNonDeserializingLongReceiver(InputGate inputGate) {
			super();
			this.inputGate = inputGate;
		}

		protected void readRecords(long remaining) throws Exception {
			LOG.debug("readRecords(remaining = {})", remaining);
			// assume LongValue instances here (4 bytes length, 8 bytes long)
			final long expectedBytes = remaining * 12;

			long readBytes = 0;

			while (running && readBytes < expectedBytes) {
				BufferOrEvent input = inputGate.getNextBufferOrEvent();
				if (input.isBuffer()) {
					Buffer buffer = input.getBuffer();
					readBytes += buffer.getSize();
					buffer.recycle();
				}
			}

		}
	}

	// ------------------------------------------------------------------------
	//  Setup Utilities
	// ------------------------------------------------------------------------

	private NetworkEnvironment createNettyNetworkEnvironment(
			@SuppressWarnings("SameParameterValue") int bufferPoolSize) throws Exception {

		final NetworkBufferPool bufferPool = new NetworkBufferPool(bufferPoolSize, BUFFER_SIZE, MemoryType.OFF_HEAP);

		Configuration config = new Configuration();
		config.setInteger(NUM_THREADS_SERVER, NUM_IO_THREADS);
		config.setInteger(NUM_THREADS_CLIENT, NUM_IO_THREADS);
		final NettyConnectionManager nettyConnectionManager = new NettyConnectionManager(
			new NettyConfig(LOCAL_ADDRESS, 0, BUFFER_SIZE, 1, config));

		return new NetworkEnvironment(
			bufferPool,
			nettyConnectionManager,
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOMode.SYNC,
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL.defaultValue(),
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX.defaultValue(),
			TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue(),
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue());
	}

	private ResultPartitionWriter createResultPartition(
			JobID jobId,
			ResultPartitionID partitionId,
			NetworkEnvironment env) throws Exception {

		ResultPartition resultPartition = new ResultPartition(
			"sender task",
			new NoOpTaskActions(),
			jobId,
			partitionId,
			ResultPartitionType.PIPELINED_BOUNDED,
			1,
			1,
			env.getResultPartitionManager(),
			new NoOpResultPartitionConsumableNotifier(),
			ioManager,
			false);
		ResultPartitionWriter partitionWriter = new ResultPartitionWriter(
			resultPartition);

		int numBuffers = TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() +
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue();

		BufferPool bufferPool = env.getNetworkBufferPool().createBufferPool(1, numBuffers);
		resultPartition.registerBufferPool(bufferPool);

		env.getResultPartitionManager().registerResultPartition(resultPartition);

		return partitionWriter;
	}

	private SingleInputGate createInputGate(
			JobID jobId,
			IntermediateDataSetID dataSetID,
			ResultPartitionID consumedPartitionId,
			ExecutionAttemptID executionAttemptID,
			TaskManagerLocation senderLocation,
			NetworkEnvironment env) throws IOException {

		final InputChannelDeploymentDescriptor channelDescr = new InputChannelDeploymentDescriptor(
			consumedPartitionId,
			ResultPartitionLocation.createRemote(new ConnectionID(senderLocation, 0)));

		final InputGateDeploymentDescriptor gateDescr = new InputGateDeploymentDescriptor(
			dataSetID,
			ResultPartitionType.PIPELINED_BOUNDED,
			0,
			new InputChannelDeploymentDescriptor[] { channelDescr } );

		SingleInputGate gate = SingleInputGate.create(
			"receiving task",
			jobId,
			executionAttemptID,
			gateDescr,
			env,
			new NoOpTaskActions(),
			new DummyTaskIOMetricGroup());

		int numBuffers = TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() +
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue();

		BufferPool bufferPool =
			env.getNetworkBufferPool().createBufferPool(1, numBuffers);

		gate.setBufferPool(bufferPool);

		return gate;
	}

	// ------------------------------------------------------------------------
	//  Mocks
	// ------------------------------------------------------------------------

	/**
	 * A dummy implementation of the {@link TaskActions}. We implement this here rather than using Mockito
	 * to avoid using mockito in this benchmark class.
	 */
	private static final class NoOpTaskActions implements TaskActions {

		@Override
		public void triggerPartitionProducerStateCheck(
			JobID jobId,
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId) {}

		@Override
		public void failExternally(Throwable cause) {}
	}

	private static final class NoOpResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

		@Override
		public void notifyPartitionConsumable(JobID j, ResultPartitionID p, TaskActions t) {}
	}
}
