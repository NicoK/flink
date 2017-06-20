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

import static org.apache.flink.runtime.blob.BlobClientTest.prepareTestFile;
import static org.apache.flink.runtime.blob.BlobClientTest.validateGet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class contains unit tests for the {@link BlobClient} with ssl enabled.
 */
public class BlobClientSslTest extends TestLogger {

	/** The instance of the SSL BLOB server used during the tests. */
	private static BlobServer BLOB_SSL_SERVER;

	/** The SSL blob service client configuration */
	private static Configuration sslClientConfig;

	/** The instance of the non-SSL BLOB server used during the tests. */
	private static BlobServer BLOB_SERVER;

	/** The non-ssl blob service client configuration */
	private static Configuration clientConfig;

	/**
	 * Starts the SSL enabled BLOB server.
	 */
	@BeforeClass
	public static void startSSLServer() throws IOException {
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
		config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
		BLOB_SSL_SERVER = new BlobServer(config, new VoidDistributedBlobStore());


		sslClientConfig = new Configuration();
		sslClientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		sslClientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		sslClientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
	}

	/**
	 * Starts the SSL disabled BLOB server.
	 */
	@BeforeClass
	public static void startNonSSLServer() throws IOException {
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		config.setBoolean(BlobServerOptions.SSL_ENABLED, false);
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
		config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
		BLOB_SERVER = new BlobServer(config, new VoidDistributedBlobStore());

		clientConfig = new Configuration();
		clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		clientConfig.setBoolean(BlobServerOptions.SSL_ENABLED, false);
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
	}

	/**
	 * Shuts the BLOB server down.
	 */
	@AfterClass
	public static void stopServers() throws IOException {
		if (BLOB_SSL_SERVER != null) {
			BLOB_SSL_SERVER.close();
		}

		if (BLOB_SERVER != null) {
			BLOB_SERVER.close();
		}
	}

	/**
	 * Tests the PUT/GET operations for content-addressable streams.
	 */
	@Test
	public void testContentAddressableStream() {

		BlobClient client = null;
		InputStream is = null;

		try {
			File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();

			BlobKey origKey = prepareTestFile(testFile);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SSL_SERVER.getPort());
			client = new BlobClient(serverAddress, sslClientConfig);

			// Store the data
			is = new FileInputStream(testFile);
			BlobKey receivedKey = client.put(is);
			assertEquals(origKey, receivedKey);

			is.close();
			is = null;

			// Retrieve the data
			is = client.get(receivedKey);
			validateGet(is, testFile);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (is != null) {
				try {
					is.close();
				} catch (Throwable t) {}
			}
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {}
			}
		}
	}

	/**
	 * Tests the static {@link BlobClient#uploadJarFiles(InetSocketAddress, Configuration, List)} helper.
	 */
	private void uploadJarFile(BlobServer blobServer, Configuration blobClientConfig) throws Exception {
		final File testFile = File.createTempFile("testfile", ".dat");
		testFile.deleteOnExit();
		prepareTestFile(testFile);

		InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());

		List<BlobKey> blobKeys = BlobClient.uploadJarFiles(serverAddress, blobClientConfig,
			Collections.singletonList(new Path(testFile.toURI())));

		assertEquals(1, blobKeys.size());

		try (BlobClient blobClient = new BlobClient(serverAddress, blobClientConfig)) {
			InputStream is = blobClient.get(blobKeys.get(0));
			validateGet(is, testFile);
		}
	}

	/**
	 * Verify ssl client to ssl server upload
	 */
	@Test
	public void testUploadJarFilesHelper() throws Exception {
		uploadJarFile(BLOB_SSL_SERVER, sslClientConfig);
	}

	/**
	 * Verify ssl client to non-ssl server failure
	 */
	@Test
	public void testSSLClientFailure() throws Exception {
		try {
			uploadJarFile(BLOB_SERVER, sslClientConfig);
			fail("SSL client connected to non-ssl server");
		} catch (Exception e) {
			// Exception expected
		}
	}

	/**
	 * Verify non-ssl client to ssl server failure
	 */
	@Test
	public void testSSLServerFailure() throws Exception {
		try {
			uploadJarFile(BLOB_SSL_SERVER, clientConfig);
			fail("Non-SSL client connected to ssl server");
		} catch (Exception e) {
			// Exception expected
		}
	}

	/**
	 * Verify non-ssl connection sanity
	 */
	@Test
	public void testNonSSLConnection() throws Exception {
		uploadJarFile(BLOB_SERVER, clientConfig);
	}
}
