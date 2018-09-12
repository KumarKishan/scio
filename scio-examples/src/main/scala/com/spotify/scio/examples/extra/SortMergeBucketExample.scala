package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.values.SortMergeBucketFunctions._

import scala.util.Random

class SortMergeBucketExample {}

object SMBWrite {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(1 to 1000)
      .map(i => TestRecord.newBuilder().setIntField(i).build())
      .keyBy(_ => Random.nextInt(100))
      .writeSMB(args("output"), 10)

    sc.close()
  }
}

object SMBRead{
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.readAndJoin(
      AvroPath[Int, TestRecord](args("left")),
      AvroPath[Int, TestRecord](args("right"))
    ).saveAsTextFile(args("output"))
  }
}
