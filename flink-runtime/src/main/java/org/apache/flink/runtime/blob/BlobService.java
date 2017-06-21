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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * A simple store and retrieve binary large objects (BLOBs).
 */
public interface BlobService extends Closeable {

	/**
	 * Returns the path to a local copy of the file associated with the provided job ID and blob key.
	 *
	 * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key blob key associated with the requested file
	 * @return The path to the file.
	 * @throws java.io.FileNotFoundException when the path does not exist;
	 * @throws IOException if any other error occurs when retrieving the file
	 */
	File getFile(@Nullable JobID jobId, BlobKey key) throws IOException;

	/**
	 * Deletes the file associated with the provided job ID and blob key.
	 *
	 * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key associated with the file to be deleted
	 * @throws IOException
	 */
	void delete(@Nullable JobID jobId, BlobKey key) throws IOException;

	/**
	 * Returns the port of the blob service.
	 * @return the port of the blob service.
	 */
	int getPort();

	BlobClient createClient() throws IOException;
}
