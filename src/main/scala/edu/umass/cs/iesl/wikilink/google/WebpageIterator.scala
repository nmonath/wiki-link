package edu.umass.cs.iesl.wikilink.google

import java.io.{FileReader, BufferedReader}
import cc.refectorie.user.sameer.util.CmdLine

/**
 * @author sameer
 * @date 3/8/12
 */

class WebpageIterator(val filename: String, val takeOnly: Int = Int.MaxValue) extends Iterator[Webpage] {
  val reader = new BufferedReader(new FileReader(filename))
  var count = 0

  def hasNext = {
    val bool = count < takeOnly && reader.ready
    if (!bool) reader.close
    bool
  }

  def next() = {
    count += 1
    Webpage.getNext(reader)
  }
}

object WebpageIterator {
  def main(args: Array[String]) {
    val opts = CmdLine.parse(args)
    println(opts)
    val filename = opts.getOrElse("file", "/Users/sameer/tmp/input")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val iterator = new WebpageIterator(filename, takeOnly)
    iterator.foreach(w => println(w))
  }
}