/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.runtime.jdbc

import java.io
import java.io._
import java.nio.file.{Files, Paths}

import scala.reflect.io.File


private[deequ] object LocalDiskUtils {

  def exists(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }

  def delete(path: String): Unit = {
    File(path).delete()
  }

  def rename(pathFrom: String, pathTo: String): Unit = {
    new io.File(pathFrom).renameTo(new io.File(pathTo))
  }

  /* Helper function to read from a binary file on Disk */
  def readFromFileOnDisk[T](path: String)
                           (readFunc: DataInputStream => T): T = {

    val input = new DataInputStream(new FileInputStream(path))

    try {
      readFunc(input)
    } finally {
      if (input != null) {
        input.close()
      }
    }
  }

  /* Helper function to write to a binary file on Disk */
  def writeToFileOnDisk(path: String, overwrite: Boolean = false)
                       (writeFunc: DataOutputStream => Unit): Unit = {

    val output = new DataOutputStream(new FileOutputStream(path))

    try {
      writeFunc(output)
    } finally {
      if (output != null) {
        output.close()
      }
    }
  }
}
