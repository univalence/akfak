package akfak

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import scala.annotation.tailrec
import scala.util.Using

object ServerMain {
  def main(args: Array[String]): Unit = {
    Using(ServerSocketChannel.open()) { serverSocket =>
      serverSocket.bind(new InetSocketAddress(19092))

      serviceLoop(serverSocket)
    }.get
  }

  @tailrec
  def serviceLoop(serverSocket: ServerSocketChannel): Unit = {
    val clientSocket = serverSocket.accept()

    val headerBuffer = ByteBuffer.allocate(Header.byteSize)
    clientSocket.read(headerBuffer)
    headerBuffer.rewind()
    val header = Header.readFrom(headerBuffer)

    if (header.magic != Header.Magic)
      throw new IllegalArgumentException(s"bad magic number: ${header.magic}")

    val requestBuffer = ByteBuffer.allocate(header.payloadSize)
    clientSocket.read(requestBuffer)
    requestBuffer.rewind()

    val request = deserialize(requestBuffer)
    val response = processRequest(request)
    val responseBuffer = serialize(response)

    clientSocket.write(responseHeaderBuffer)
    clientSocket.write(responseBuffer)

    serviceLoop(serverSocket)
  }

  def deserialize(buffer: ByteBuffer): Request = {
    val size = buffer.getInt()
    val data = Array.ofDim[Byte](size)
    buffer.get(data)

    val message = new String(data)

    Request.EchoRequest(message)
  }

  def serialize(response: Response): ByteBuffer = ???

  def processRequest(request: Request): Response = request match {
    case Request.EchoRequest(message) => Response.EchoResponse(message)
  }

  def parse(buffer: ByteBuffer): Request = ???
}

object ClientMain {}

case class Header(magic: Int, payloadType: Int, payloadSize: Int)
object Header {
  val Magic = 0xdeadbeef

  val byteSize: Int = 4 + 4 + 4

  def readFrom(buffer: ByteBuffer): Header =
    Header(
      magic = buffer.getInt(),
      payloadType = buffer.getInt(),
      payloadSize = buffer.getInt()
    )
}

sealed trait Request
object Request {
  case class EchoRequest(message: String) extends Request
}

sealed trait Response
object Response {
  case class EchoResponse(message: String) extends Response
}