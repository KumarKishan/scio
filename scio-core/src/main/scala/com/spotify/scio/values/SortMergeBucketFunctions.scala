package com.spotify.scio.values

import java.io.File
import java.nio.channels.{Channels, WritableByteChannel}

import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema.Field
import org.apache.avro.file.{CodecFactory, DataFileReader, DataFileWriter}
import org.apache.avro.generic._
import org.apache.avro.{Schema, generic}
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.util.gcsfs.GcsPath

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object SortMergeBucketFunctions {

  private def kvSchema[T <: IndexedRecord : ClassTag]: Schema = {
    val fields = scala.collection.immutable.List(
      new Field("key", Schema.create(Schema.Type.STRING), "", ""),
      new Field("value",
        ScioUtil.classOf[T].getMethod("getClassSchema").invoke(null)
          .asInstanceOf[Schema],
        "", ""))

    val schema = Schema.createRecord("kvSchema", null, "com.spotify.scio", false)
    schema.setFields(fields.asJava)
    schema
  }

  private def partitionFiles(files: Array[File]): Map[Int, File] = {
    val bucketRegex = ".*bucket-(\\d+).avro".r
    files.toList.groupBy { file =>
      bucketRegex
        .findFirstMatchIn(file.getName)
        .map(_.group(1).toInt).getOrElse(-1)
    }.mapValues(_.head)
  }

  private def getResource(path: String, isTest: Boolean = false): ResourceId = {
    if (isTest) {
      LocalResources.fromString(path, false)
    } else {
      val gcsPath = GcsPath.fromUri(path)
      // @TODO: use GCS FS
      null
    }
  }

  // scalastyle:off line.size.limit
  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  case class AvroPath[K, T](path: String)

  implicit class SortMergeBucketJoin(val self: ScioContext) {
    def readAndJoin[K: ClassTag, T1 <: IndexedRecord : ClassTag, T2 <: IndexedRecord : ClassTag](
      path1: AvroPath[K, T1],
      path2: AvroPath[K, T2]
    )(implicit ordering: Ordering[K]): SCollection[(K, (T1, T2))] = {
      val (s1, s2) = (kvSchema[T1], kvSchema[T2])
      val (f1, f2) = (new File(path1.path).listFiles(), new File(path2.path).listFiles())
      val (p1, p2) = (partitionFiles(f1), partitionFiles(f2))

      val matches = (p2.toSeq ++ p1.toSeq)
        .groupBy(_._1)
        .filter(_._2.size == 2) // this bucket must exist in both right and left sides
        .mapValues(files => (files(0)._2, files(1)._2))
        .map { case (bucket, (left, right)) =>
          val l = new DataFileReader(left, new generic.GenericDatumReader[GenericRecord](s1))
          val r = new DataFileReader(right, new generic.GenericDatumReader[GenericRecord](s2))
          val output = ListBuffer.empty[(K, (T1, T2))]

          while (l.hasNext && r.hasNext) {
            var (leftBlock, rightBlock) = (l.next, r.next)
            var leftKey = leftBlock.get("key").toString.asInstanceOf[K]
            var rightKey = rightBlock.get("key").toString.asInstanceOf[K]

            val comparison = ordering.compare(leftKey, rightKey)
            if (comparison == 0) {
              output.append((leftKey, (leftBlock.get("value").asInstanceOf[T1], rightBlock.get("value").asInstanceOf[T2])))

              while (l.hasNext && {
                leftBlock = l.next()
                leftKey = leftBlock.get("key").toString.asInstanceOf[K]
                leftKey == rightKey
              }) {
                output.append((leftKey, (leftBlock.get("value").asInstanceOf[T1], rightBlock.get("value").asInstanceOf[T2])))
              }

              while (r.hasNext && {
                rightBlock = r.next()
                rightKey = rightBlock.get("key").toString.asInstanceOf[K]
                leftKey == rightKey
              }) {
                output.append((leftKey, (leftBlock.get("value").asInstanceOf[T1], rightBlock.get("value").asInstanceOf[T2])))
              }
            }
            else if (comparison < 0) {
              leftBlock = l.next()
            }
            else {
              rightBlock = r.next()
            }
          }
          self.parallelize(output.toList)
        }

      SCollection.unionAll(matches)
    }
  }

  implicit class SortMergeBucketWrite[K, V <: IndexedRecord : ClassTag](
                                                                         val self: SCollection[(K, V)]
                                                                       ) extends Serializable {
    def writeSMB(
                  path: String, numBuckets: Int
                )(implicit hashFn: K => Int, ord: Ordering[K]) {
      self
        .groupBy(kv => hashFn(kv._1) % numBuckets)
        .map { case (bucket, elements) =>
          val outputSchema = kvSchema[V]

          val outputResource = getResource(s"$path/bucket-$bucket.avro")
          val sink = new GenericRecordAvroSink(outputResource, outputSchema)
          val writer = sink.createWriteOperation().createWriter()

          writer.open(outputResource.toString)

          elements.toList.sortBy(_._1).foreach { case (k, v) =>
            writer.write(
              new GenericRecordBuilder(outputSchema).set("key", k.toString).set("value", v).build()
            )
          }

          writer.close()
        }
    }

    class GenericRecordAvroSink(
                                 outputTarget: ValueProvider[ResourceId],
                                 schema: Schema,
                                 dataFileWriter: DataFileWriter[GenericRecord]
                               ) extends FileBasedSink[GenericRecord, Unit, GenericRecord](
      outputTarget,
      new DynamicDestinations[GenericRecord, Unit, GenericRecord] {
        override def formatRecord(record: GenericRecord): GenericRecord = record
        override def getDestination(element: GenericRecord): Unit = {}
        override def getDefaultDestination: Unit = {}
        override def getFilenamePolicy(destination: Unit): FileBasedSink.FilenamePolicy =
          DefaultFilenamePolicy.fromStandardParameters(outputTarget, null, ".avro", false)
      },
      Compression.UNCOMPRESSED) with Serializable {

      def this(outputTarget: ResourceId, schema: Schema) {
        this(
          StaticValueProvider.of(outputTarget),
          schema,
          new DataFileWriter[GenericRecord](
            new GenericDatumWriter[GenericRecord](schema)).setCodec(CodecFactory.snappyCodec())
        )
      }

      override def createWriteOperation(): FileBasedSink.WriteOperation[Unit, GenericRecord] = {
        new FileBasedSink.WriteOperation[Unit, GenericRecord](this) {

          override def createWriter(): FileBasedSink.Writer[Unit, GenericRecord] =
            new FileBasedSink.Writer[Unit, GenericRecord](this, MimeTypes.BINARY) {
              override def prepareWrite(channel: WritableByteChannel): Unit =
                dataFileWriter.create(schema, Channels.newOutputStream(channel))
              override def write(value: GenericRecord): Unit = dataFileWriter.append(value)
              override def finishWrite(): Unit = dataFileWriter.flush()
            }
        }
      }
    }
  }
  // scalastyle:on line.size.limit
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length
}
