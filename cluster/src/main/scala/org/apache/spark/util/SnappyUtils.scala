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

// scalastyle:off println

package org.apache.spark.util

import java.security.SecureClassLoader

import com.pivotal.gemfirexd.internal.engine.Misc

object SnappyUtils {

  def getSnappyStoreContextLoader(parent: ClassLoader): ClassLoader =
    new SecureClassLoader(parent) {
    override def loadClass(name: String): Class[_] = {
      try {
        //try to load from parent
        super.loadClass(name)
      } catch {
        case cnfe: ClassNotFoundException =>
          Misc.getMemStore.getDatabase.getClassFactory.loadApplicationClass(name)
      }
    }
  }
}





