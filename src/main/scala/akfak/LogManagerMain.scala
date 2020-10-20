package akfak

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{
  Files,
  Path,
  Paths,
  StandardCopyOption,
  StandardOpenOption
}
import java.util.UUID
import scala.collection.mutable
import scala.io.Source
import scala.util.Using
import scala.util.hashing.MurmurHash3

object LogManagerMain {
  def main(args: Array[String]): Unit = {
    val workDir = Paths.get("stream")
    if (!workDir.toFile.exists()) Files.createDirectories(workDir)

    val broker = new Broker(workDir)

    if (!broker.topics.contains("my-topic"))
      broker.createTopic("my-topic", 4)

    for (i <- 0 to 10) {
      val key = s"key-${i % 10}"
      val value = s"value:${UUID.randomUUID()}"
      println(s"send: $key - $value")
      broker.send(
        "my-topic",
        key.getBytes(StandardCharsets.UTF_8),
        value.getBytes(StandardCharsets.UTF_8)
      )
    }

    val records = broker.poll("my-topic", 1, 0)
    for {
      (k, v) <- records
    } {
      println(s"poll: ${new String(k)} - ${new String(v)}")
    }
  }
}

class Broker(workDir: Path) {
  val checkpointFile: CheckpointFile = {
    val file = workDir.resolve("checkpoints")
    if (!file.toFile.exists()) Files.createFile(file)

    new CheckpointFile(file)
  }

  val topics: mutable.Map[String, Seq[Partition]] =
    mutable.Map.from {
      checkpointFile.checkpoints.toSeq
        .map { case (tp, _) => tp.topic -> tp.partitionId }
        .groupBy { case (t, _) => t }
        .view
        .mapValues(_.map {
          case (t, pId) =>
            createPartition(t, pId)
        }.toSeq)
    }

  def createTopic(topic: String, partitionCount: Int): Unit = {
    val partitions: Seq[Partition] =
      (0 until partitionCount)
        .map { partitionId =>
          checkpointFile.update(topic, partitionId, 0)
          createPartition(topic, partitionId)
        }

    topics += (topic -> partitions)
  }

  private def createPartition(topic: String, partitionId: Int) = {
    val partitionDir = workDir.resolve(s"$topic-$partitionId")
    if (!partitionDir.toFile.exists())
      Files.createDirectories(partitionDir)

    new Partition(topic, partitionId, partitionDir, checkpointFile)
  }

  def poll(
      topic: String,
      partitionId: Int,
      offset: Int
  ): List[(Array[Byte], Array[Byte])] = {
    val partition = topics(topic)(partitionId)

    partition.poll(offset: Int)
  }

  def send(topic: String, key: Array[Byte], value: Array[Byte]): Unit = {
    val partitions = topics(topic)
    val partitionId = MurmurHash3.bytesHash(key).abs % partitions.size
    val partition = partitions(partitionId)

    val offset = checkpointFile.getOffset(topic, partitionId)
    val newOffset = partition.append(key, value, offset)
    checkpointFile.update(topic, partitionId, newOffset)
  }

}

class Partition(
    topic: String,
    partitionId: Int,
    partitionDir: Path,
    checkpointFile: CheckpointFile
) {
  val indexFile: IndexFile = {
    val file = partitionDir.resolve("partition.idx")
    if (!file.toFile.exists()) Files.createFile(file)

    new IndexFile(file)
  }

  val logFile: LogFile = {
    val file = partitionDir.resolve("partition.log")
    if (!file.toFile.exists()) Files.createFile(file)

    new LogFile(file)
  }

  def poll(offset: Int): List[(Array[Byte], Array[Byte])] = {
    val maxOffset = checkpointFile.getOffset(topic, partitionId)

    (offset until maxOffset)
      .map(ofs => indexFile.getPositionOf(ofs))
      .map(position => logFile.getRecord(position))
      .toList
  }

  def append(key: Array[Byte], value: Array[Byte], offset: Int): Int = {
    val position = logFile.append(key, value)
    indexFile.append(offset, position)

    offset + 1
  }
}

class LogFile(file: Path) {
  def append(key: Array[Byte], value: Array[Byte]): Long =
    Using(FileChannel.open(file, StandardOpenOption.APPEND)) { channel =>
      val position = channel.position()
      val buffer = ByteBuffer.allocate(4 + 4 + key.length + value.length)
      buffer.putInt(key.length)
      buffer.putInt(value.length)
      buffer.put(key)
      buffer.put(value)
      buffer.rewind()
      channel.write(buffer)

      position
    }.get

  def getRecord(position: Long): (Array[Byte], Array[Byte]) =
    Using(FileChannel.open(file, StandardOpenOption.READ)) { channel =>
      channel.position(position)

      val keyValueSizeBuffer = ByteBuffer.allocate(4 + 4)
      channel.read(keyValueSizeBuffer)
      keyValueSizeBuffer.rewind()
      val keySize = keyValueSizeBuffer.getInt()
      val valueSize = keyValueSizeBuffer.getInt()

      val keyBuffer = ByteBuffer.allocate(keySize)
      channel.read(keyBuffer)
      keyBuffer.rewind()

      val valueBuffer = ByteBuffer.allocate(valueSize)
      channel.read(valueBuffer)
      valueBuffer.rewind()

      (keyBuffer.array(), valueBuffer.array())
    }.get
}

class IndexFile(file: Path) {
  def append(offset: Int, position: Long): Unit =
    Using(FileChannel.open(file, StandardOpenOption.APPEND)) { channel =>
      val data = s"$offset,$position\n"
      val buffer = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8))
      channel.write(buffer)
    }

  def getPositionOf(offset: Int): Long = {
    Using(Source.fromFile(file.toFile)) { content =>
      val positions =
        (for (line <- content.getLines()) yield {
          val fields = line.split(",", 2)
          fields(0).toInt -> fields(1).toLong
        }).toMap

      positions(offset)
    }.get
  }
}

class CheckpointFile(file: Path) {
  val checkpoints: mutable.Map[TopicPartition, Int] =
    mutable.Map.from(readCheckpoints())

  def getOffset(topic: String, partitionId: Int): Int =
    checkpoints(TopicPartition(topic, partitionId))

  def update(topic: String, partitionId: Int, offset: Int): Unit = {
    checkpoints += (TopicPartition(topic, partitionId) -> offset)
    writeCheckpoints()
  }

  def writeCheckpoints(): Unit = {
    val file = File.createTempFile("checkpoints", ".tmp")
    Using(FileChannel.open(file.toPath, StandardOpenOption.WRITE)) { channel =>
      checkpoints.foreach {
        case (tp, offset) =>
          val buffer = ByteBuffer.wrap(
            s"${tp.topic},${tp.partitionId},$offset\n"
              .getBytes(StandardCharsets.UTF_8)
          )
          channel.write(buffer)
      }
    }

    try {
      Files.copy(file.toPath, this.file, StandardCopyOption.ATOMIC_MOVE)
    } catch {
      case _: Exception =>
        Files.copy(file.toPath, this.file, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  def readCheckpoints(): Map[TopicPartition, Int] =
    Using(Source.fromFile(file.toFile)) { content =>
      (for (line <- content.getLines()) yield {
        val fields = line.split(",", 3)
        TopicPartition(fields(0), fields(1).toInt) -> fields(2).toInt
      }).toList
    }.get.toMap
}
case class TopicPartition(topic: String, partitionId: Int)
