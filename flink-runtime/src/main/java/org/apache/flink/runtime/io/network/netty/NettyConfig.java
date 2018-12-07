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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NetworkEnvironmentOptions;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConfig {

	private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

	enum TransportType {
		NIO, EPOLL, AUTO
	}

	static final String SERVER_THREAD_GROUP_NAME = "Flink Netty Server";

	static final String CLIENT_THREAD_GROUP_NAME = "Flink Netty Client";

	/**
	 * Arenas allocate chunks of pageSize << maxOrder bytes. With these defaults, this results in
	 * chunks of 4 MB.
	 *
	 * @see NetworkEnvironmentOptions#NUM_PAGES_MAX_ORDER
	 */
	static final int DEFAULT_PAGE_SIZE = 8192;

	private final InetAddress serverAddress;

	private final int serverPort;

	private final int memorySegmentSize;

	private final int numberOfSlots;

	private final Configuration config; // optional configuration

	public NettyConfig(
			InetAddress serverAddress,
			int serverPort,
			int memorySegmentSize,
			int numberOfSlots,
			Configuration config) {

		this.serverAddress = checkNotNull(serverAddress);

		checkArgument(serverPort >= 0 && serverPort <= 65536, "Invalid port number.");
		this.serverPort = serverPort;

		checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
		this.memorySegmentSize = memorySegmentSize;

		checkArgument(numberOfSlots > 0, "Number of slots");
		this.numberOfSlots = numberOfSlots;

		this.config = checkNotNull(config);

		LOG.info(this.toString());
	}

	InetAddress getServerAddress() {
		return serverAddress;
	}

	int getServerPort() {
		return serverPort;
	}

	int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	// ------------------------------------------------------------------------
	// Getters
	// ------------------------------------------------------------------------

	public int getServerConnectBacklog() {
		return config.getInteger(NetworkEnvironmentOptions.CONNECT_BACKLOG);
	}

	public int getNumberOfArenas() {
		// default: number of slots
		final int configValue = config.getInteger(NetworkEnvironmentOptions.NUM_ARENAS);
		return configValue == -1 ? numberOfSlots : configValue;
	}

	public int getServerNumThreads() {
		// default: number of task slots
		final int configValue = config.getInteger(NetworkEnvironmentOptions.NUM_THREADS_SERVER);
		return configValue == -1 ? numberOfSlots : configValue;
	}

	public int getClientNumThreads() {
		// default: number of task slots
		final int configValue = config.getInteger(NetworkEnvironmentOptions.NUM_THREADS_CLIENT);
		return configValue == -1 ? numberOfSlots : configValue;
	}

	public int getClientConnectTimeoutSeconds() {
		return config.getInteger(NetworkEnvironmentOptions.CLIENT_CONNECT_TIMEOUT_SECONDS);
	}

	public int getSendAndReceiveBufferSize() {
		return config.getInteger(NetworkEnvironmentOptions.SEND_RECEIVE_BUFFER_SIZE);
	}

	public TransportType getTransportType() {
		String transport = config.getString(NetworkEnvironmentOptions.TRANSPORT_TYPE);

		switch (transport) {
			case "nio":
				return TransportType.NIO;
			case "epoll":
				return TransportType.EPOLL;
			default:
				return TransportType.AUTO;
		}
	}

	@Nullable
	public SSLHandlerFactory createClientSSLEngineFactory() throws Exception {
		return getSSLEnabled() ?
				SSLUtils.createInternalClientSSLEngineFactory(config) :
				null;
	}

	@Nullable
	public SSLHandlerFactory createServerSSLEngineFactory() throws Exception {
		return getSSLEnabled() ?
				SSLUtils.createInternalServerSSLEngineFactory(config) :
				null;
	}

	public boolean getSSLEnabled() {
		return config.getBoolean(NetworkEnvironmentOptions.DATA_SSL_ENABLED)
			&& SSLUtils.isInternalSSLEnabled(config);
	}

	public int getPageSize() {
		return DEFAULT_PAGE_SIZE;
	}

	public int getMaxOrder() {
		int maxOrder = config.getInteger(NetworkEnvironmentOptions.NUM_PAGES_MAX_ORDER);

		// Assert the chunk size is not too small to fulfill the requirements of a single thread.
		// We require the chunk size to be larger than 1MB based on the experiment results.
		int minimumMaxOrder = 20 - MathUtils.log2strict(getPageSize());

		return Math.max(minimumMaxOrder, maxOrder);
	}

	public Configuration getConfig() {
		return config;
	}

	@Override
	public String toString() {
		String format = "NettyConfig [" +
				"server address: %s, " +
				"server port: %d, " +
				"ssl enabled: %s, " +
				"memory segment size (bytes): %d, " +
				"transport type: %s, " +
				"number of server threads: %d (%s), " +
				"number of client threads: %d (%s), " +
				"server connect backlog: %d (%s), " +
				"client connect timeout (sec): %d, " +
				"send/receive buffer size (bytes): %d (%s)]";

		String def = "use Netty's default";
		String man = "manual";

		return String.format(format, serverAddress, serverPort, getSSLEnabled() ? "true" : "false",
				memorySegmentSize, getTransportType(), getServerNumThreads(),
				getServerNumThreads() == 0 ? def : man,
				getClientNumThreads(), getClientNumThreads() == 0 ? def : man,
				getServerConnectBacklog(), getServerConnectBacklog() == 0 ? def : man,
				getClientConnectTimeoutSeconds(), getSendAndReceiveBufferSize(),
				getSendAndReceiveBufferSize() == 0 ? def : man);
	}
}
