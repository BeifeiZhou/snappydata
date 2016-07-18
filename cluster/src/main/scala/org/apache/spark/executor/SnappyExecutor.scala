/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.executor

import java.net.{URL}
import org.apache.spark.SparkEnv
import org.apache.spark.util.MutableURLClassLoader
import org.apache.spark.util.classloader.DynamicURLClassLoader

class SnappyExecutor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false) extends Executor(executorId, executorHostname, env, userClassPath) {

  override def createClassLoader(urls: Array[URL],
      parentLoader: ClassLoader, userClassPathFirst: Boolean): MutableURLClassLoader = {
    new DynamicURLClassLoader(urls, parentLoader, parentFirst = !userClassPathFirst)
  }
}
