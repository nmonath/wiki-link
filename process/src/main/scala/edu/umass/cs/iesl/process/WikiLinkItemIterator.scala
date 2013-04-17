package edu.umass.cs.iesl.process

import edu.umass.cs.iesl.wiki.WikiLinkItem
import java.io.File
import org.apache.thrift.transport.TTransportException

class WikiLinkItemIterator(f: File) extends Iterator[WikiLinkItem] {

  var done = false
  val (stream, proto) = ThriftSerializerFactory.getReader(f)
  private var _next: Option[WikiLinkItem] = getNext()

  private def getNext(): Option[WikiLinkItem] = try { 
    Some(WikiLinkItem.decode(proto))
  } catch { case _: TTransportException => { done = true; stream.close(); None }}

  def hasNext(): Boolean = !done && (_next != None || { _next = getNext(); _next != None })

  def next(): WikiLinkItem = if (hasNext()) _next match {
    case Some(wli) => { _next = None; wli }
    case None => { throw new Exception("Next on empty iterator.") }
  } else throw new Exception("Next on empty iterator.")

}