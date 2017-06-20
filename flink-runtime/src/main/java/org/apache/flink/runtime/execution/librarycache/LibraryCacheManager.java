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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;

/**
 * Bridge between a library storage service and the class loader's use of them.
 */
public interface LibraryCacheManager {

	/**
	 * Returns the user code class loader associated with jobId.
	 *
	 * @param jobId
	 * 		identifying the job
	 *
	 * @return ClassLoader which can load the user code
	 */
	ClassLoader getClassLoader(JobID jobId);

	/**
	 * Returns a file handle to the file identified by the blob key.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param blobKey
	 * 		identifying the requested file
	 *
	 * @return File handle
	 *
	 * @throws IOException
	 * 		if any error occurs when retrieving the file
	 * 	@deprecated remove this function?!
	 */
	@Deprecated
	File getFile(JobID jobId, BlobKey blobKey) throws IOException;

	/**
	 * Registers a job with its required jar files and classpaths. The jar files are identified by
	 * their blob keys.
	 *
	 * @param jobId
	 * 		job ID
	 * @param requiredJarFiles
	 * 		collection of blob keys identifying the required jar files
	 * @param requiredClasspaths
	 * 		collection of classpaths that are added to the user code class loader
	 *
	 * @throws IOException
	 * 		if any error occurs when retrieving the required jar files
	 * @see #unregisterJob(JobID) counterpart of this method
	 */
	void registerJob(JobID jobId, Collection<BlobKey> requiredJarFiles, Collection<URL> requiredClasspaths)
			throws IOException;

	/**
	 * Registers a job task execution with its required jar files and classpaths. The jar files are
	 * identified by their blob keys.
	 *
	 * @param jobId
	 * 		job ID
	 * @param requiredJarFiles
	 * 		collection of blob keys identifying the required jar files
	 * @param requiredClasspaths
	 * 		collection of classpaths that are added to the user code class loader
	 *
	 * @throws IOException
	 * @see #unregisterTask(JobID, ExecutionAttemptID) counterpart of this method
	 */
	void registerTask(JobID jobId, ExecutionAttemptID execution, Collection<BlobKey> requiredJarFiles,
			Collection<URL> requiredClasspaths) throws IOException;

	/**
	 * Unregisters a job task execution from the library cache manager.
	 * <p>
	 * <strong>Note:</strong> this is the counterpart of {@link #registerTask(JobID,
	 * ExecutionAttemptID, Collection, Collection)} and it will not remove any job added via
	 * {@link #registerJob(JobID, Collection, Collection)}!
	 *
	 * @param jobId
	 * 		job ID
	 *
	 * @see #registerTask(JobID, ExecutionAttemptID, Collection, Collection) counterpart of this
	 * method
	 */
	void unregisterTask(JobID jobId, ExecutionAttemptID execution);

	/**
	 * Unregisters a job from the library cache manager.
	 * <p>
	 * <strong>Note:</strong> this is the counterpart of {@link #registerJob(JobID, Collection,
	 * Collection)} and it will not remove any job task execution added via {@link
	 * #registerTask(JobID, ExecutionAttemptID, Collection, Collection)}!
	 *
	 * @param jobId
	 * 		job ID
	 *
	 * @see #registerJob(JobID, Collection, Collection) counterpart of this method
	 */
	void unregisterJob(JobID jobId);

	/**
	 * Shutdown method
	 *
	 * @throws IOException
	 */
	void shutdown() throws IOException;
}
