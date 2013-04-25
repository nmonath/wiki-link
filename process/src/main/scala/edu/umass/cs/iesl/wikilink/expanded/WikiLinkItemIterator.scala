package edu.umass.cs.iesl.wikilink.expanded

import edu.umass.cs.iesl.wikilink.expanded.data.WikiLinkItem
import java.io.File
import org.apache.thrift.transport.TTransportException
import edu.umass.cs.iesl.wikilink.expanded.process.ThriftSerializerFactory

object WikiLinkItemIterator {

  def recursiveFileIterator(d: File, fileFilter: (File) => Boolean = f => true): Iterator[File] = {
    val these = d.listFiles
    these.filter(fileFilter).iterator ++ these.filter(_.isDirectory).iterator.flatMap(d => recursiveFileIterator(d, fileFilter))
  }

  def getFiles(d: File): Iterator[File] = {
    assert(d.isDirectory, d.getAbsolutePath + " is not a directory.")
    recursiveFileIterator(d, f => f.isFile && f.getName.endsWith(".gz"))
  }

  def apply(dirName: String): Iterator[WikiLinkItem] = apply(new File(dirName))

  def apply(d: File): Iterator[WikiLinkItem] = getFiles(d).flatMap(f => new PerFileWebpageIterator(f))

  def main(args: Array[String]) = {
    val it = WikiLinkItemIterator(args(0))
    var c = 0
    var m = 0
    for (wli <- it) {
      m += wli.mentions.size
      c += 1
    }
    println("Total Pages : " + c)
    println("Total Mentions : " + m)
  }
}

class PerFileWebpageIterator(f: File) extends Iterator[WikiLinkItem] {

  var done = false
  val (stream, proto) = ThriftSerializerFactory.getReader(f)
  private var _next: Option[WikiLinkItem] = getNext()

  private def getNext(): Option[WikiLinkItem] = try {
    Some(WikiLinkItem.decode(proto))
  } catch {case _: TTransportException => {done = true; stream.close(); None}}

  def hasNext(): Boolean = !done && (_next != None || {_next = getNext(); _next != None})

  def next(): WikiLinkItem = if (hasNext()) _next match {
    case Some(wli) => {_next = None; wli}
    case None => {throw new Exception("Next on empty iterator.")}
  } else throw new Exception("Next on empty iterator.")

}