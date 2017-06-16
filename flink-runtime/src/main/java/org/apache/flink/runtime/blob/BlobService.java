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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A simple store to retrieve binary large objects (BLOBs) based on a hash of their contents,
 * i.e. content-addressable BLOBs.
 */
public interface BlobService extends Closeable {

	/**
	 * Returns a local copy of the file associated with the provided blob key.
	 * <p>
	 * This will count as one entity using the file which must release it via
	 * {@link #releaseFile(JobID, BlobKey)} after use.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return The path to the local file.
	 *
	 * @throws java.io.FileNotFoundException
	 * 		if there is no such file;
	 * @throws IOException
	 * 		if any other error occurs when retrieving the file
	 */
	File getFile(JobID jobId, BlobKey key) throws IOException;

	/**
	 * Releases the file associated with the given blob key.
	 * <p>
	 * Once a file is released by all its users, it will enter staged cleanup and
	 * will eventually be deleted.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		associated with the file to be deleted
	 */
	void releaseFile(JobID jobId, BlobKey key);

	/**
	 * Deletes the file associated with the provided blob key.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		associated with the file to be deleted
	 *
	 * @return <code>true</code> if and only if the file was successfully deleted;
	 * <code>false</code> otherwise
	 */
	boolean delete(JobID jobId, BlobKey key);

	/**
	 * Returns the address (location and port) of the blob service.
	 *
	 * @return the address of the blob service.
	 */
	InetSocketAddress getAddress();
}
