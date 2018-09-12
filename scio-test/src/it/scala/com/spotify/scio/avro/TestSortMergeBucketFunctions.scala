package com.spotify.scio.avro

import java.io.File

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SortMergeBucketFunctions._
import org.apache.commons.io.FileUtils

class TestSortMergeBucketFunctions extends PipelineSpec {
  val int1 = TestRecord.newBuilder().setStringField("K1").setIntField(1).build()
  val int2 = TestRecord.newBuilder().setStringField("K2").setIntField(2).build()
  val int3 = TestRecord.newBuilder().setStringField("K2").setIntField(3).build()
  val int4 = TestRecord.newBuilder().setStringField("K3").setIntField(4).build()

  val int5 = TestRecord.newBuilder().setStringField("K1").setIntField(5).build()
  val int6 = TestRecord.newBuilder().setStringField("K1").setIntField(6).build()
  val int7 = TestRecord.newBuilder().setStringField("K2").setIntField(7).build()

  "SortMergeBucketFunctions" should "work" in {
    runWithContext { sc =>
      val left = sc.parallelize(Seq(
        int1, int2, int3, int4
      )).keyBy(_.getStringField.toString)

      val right = sc.parallelize(Seq(
        int5, int6, int7
      )).keyBy(_.getStringField.toString)

      implicit val hashFn: String => Int = _.charAt(1).toInt
      implicit val order: Ordering[String] = Ordering.String

      left.writeSMB("tmpPath", numBuckets = 3)
      right.writeSMB("tmpPath2", numBuckets = 3)

    }

    // @Todo - Figure out Avro equality matching
    val expectedOut: Iterable[(String, (TestRecord, TestRecord))] = List(
      ("K1", (int1, int5)),
      ("K1", (int1, int6)),
      ("K2", (int2, int7)),
      ("K2", (int3, int7))
    )

    runWithContext { sc =>
      sc.readAndJoin(
        AvroPath[String, TestRecord]("tmpPath"),
        AvroPath[String, TestRecord]("tmpPath2")
      ) should haveSize(4)
    }

    FileUtils.deleteDirectory(new File("tmpPath"))
    FileUtils.deleteDirectory(new File("tmpPath2"))
  }
}
