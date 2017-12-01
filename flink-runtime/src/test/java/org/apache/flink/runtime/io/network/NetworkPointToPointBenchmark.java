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

import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.types.LongValue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Context for network benchmarks executed in flink-benchmark.
 */
public class NetworkPointToPointBenchmark extends NetworkBenchmarkEnvironment<LongValue> {
	private static final long RECEIVER_TIMEOUT = 2000;

	protected Receiver receiver;
	protected RecordWriter<LongValue> recordWriter;

	public void executeThroughputBenchmark(long records) throws Exception {
		final LongValue value = new LongValue();
		value.setValue(0);

		CompletableFuture<?> recordsReceived = receiver.setExpectedRecords(records);

		for (int i = 0; i < records; i++) {
			recordWriter.emit(value);
		}
		recordWriter.flush();

		recordsReceived.get(RECEIVER_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	@Override
	public void setUp() throws Exception {
		super.setUp();

		receiver = createReceiver(SerializingLongReceiver.class);
		recordWriter = createRecordWriter();
	}

	@Override
	public void tearDown() {
		super.tearDown();

		receiver.shutdown();
	}
}
