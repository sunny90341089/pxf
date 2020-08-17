package org.greenplum.pxf.service.servlet;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.OpensslCipher;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.greenplum.pxf.service.utilities.Log4jConfigure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Listener on lifecycle events of our webapp
 */
public class ServletLifecycleListener implements ServletContextListener {

    private static final Logger LOG = LoggerFactory.getLogger(ServletContextListener.class);

	/**
	 * Called after the webapp has been initialized.
	 *
	 * 1. Initializes log4j.
	 */
	@Override
	public void contextInitialized(ServletContextEvent event) {
		// 1. Initialize log4j:
		Log4jConfigure.configure(event);

		LOG.info("PXF server webapp initialized");

		checkNativeLibraries();
	}

	/**
	 * Called before the webapp is about to go down
	 */
	@Override
	public void contextDestroyed(ServletContextEvent event) {
		LOG.info("PXF server webapp is about to go down");
	}

	/**
	 * Checks whether native libraries are loaded and logs details of the
	 * loaded libraries
	 */
	private void checkNativeLibraries() {
		boolean nativeHadoopLoaded = NativeCodeLoader.isNativeCodeLoaded();

		if (nativeHadoopLoaded) {
			Configuration conf = new Configuration();
			boolean zlibLoaded;
			boolean snappyLoaded;
			boolean zStdLoaded;
			boolean bzip2Loaded = Bzip2Factory.isNativeBzip2Loaded(conf);
			boolean openSslLoaded;

			String openSslDetail;
			String hadoopLibraryName;
			String zlibLibraryName = "";
			String snappyLibraryName = "";
			String zstdLibraryName = "";
			String lz4LibraryName;
			String bzip2LibraryName = "";

			hadoopLibraryName = NativeCodeLoader.getLibraryName();
			zlibLoaded = ZlibFactory.isNativeZlibLoaded(conf);
			if (zlibLoaded) {
				zlibLibraryName = ZlibFactory.getLibraryName();
			}
			snappyLoaded = NativeCodeLoader.buildSupportsSnappy() &&
					SnappyCodec.isNativeCodeLoaded();
			if (snappyLoaded && NativeCodeLoader.buildSupportsSnappy()) {
				snappyLibraryName = SnappyCodec.getLibraryName();
			}
			zStdLoaded = NativeCodeLoader.buildSupportsZstd() &&
					ZStandardCodec.isNativeCodeLoaded();
			if (zStdLoaded && NativeCodeLoader.buildSupportsZstd()) {
				zstdLibraryName = ZStandardCodec.getLibraryName();
			}
			if (OpensslCipher.getLoadingFailureReason() != null) {
				openSslDetail = OpensslCipher.getLoadingFailureReason();
				openSslLoaded = false;
			} else {
				openSslDetail = OpensslCipher.getLibraryName();
				openSslLoaded = true;
			}
			lz4LibraryName = Lz4Codec.getLibraryName();
			if (bzip2Loaded) {
				bzip2LibraryName = Bzip2Factory.getLibraryName(conf);
			}

			LOG.info("Hadoop native library : {} {}", true, hadoopLibraryName);
			LOG.info("zlib library          : {} {}", zlibLoaded, zlibLibraryName);
			LOG.info("snappy library        : {} {}", snappyLoaded, snappyLibraryName);
			LOG.info("zstd library          : {} {}", zStdLoaded, zstdLibraryName);
			// lz4 is linked within libhadoop
			LOG.info("lz4 library           : {} {}", true, lz4LibraryName);
			LOG.info("bzip2 library         : {} {}", bzip2Loaded, bzip2LibraryName);
			LOG.info("openssl library       : {} {}", openSslLoaded, openSslDetail);
		} else {
			LOG.info("Hadoop native library not loaded");
		}
	}
}
