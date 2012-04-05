package edu.umass.cs.iesl.wikilink.process

import java.io._
import xml.XML
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import edu.umass.cs.iesl.wikilink.google._
import cc.refectorie.user.sameer.util.CmdLine

object FilePath {
  var baseContentDir = ""
  def getPageContentStream(page: Webpage): InputStream =
    new FileInputStream("%s/%06d/%d".format(baseContentDir, page.id / 1000, page.id % 1000))
}

object Process {
  import FilePath._

  // print the wikipedia links by first projecting to <p> then to <a>
  def processPage(page: Webpage): Unit = {
    val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())
    try {
      val contentStream = getPageContentStream(page)
      val ns = parser.load(contentStream)
      ns \\ "a"
      println(ns)
    } catch {
      case e => { e.printStackTrace(); System.exit(0) }
    }
  }

  def main(args : Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    val input = opts.getOrElse("input", "/Users/sameer/tmp/input")
    val output = opts.getOrElse("output", "/Users/sameer/tmp/process-output")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val workers = opts.getOrElse("workers", "100").toInt
    val resume = opts.getOrElse("resume", "false").toBoolean

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(workers)

    baseContentDir = args(1)
    val pages = new WebpageIterator(args(0), takeOnly = 10)
    for (page <- pages)
      processPage(page)
  }

}
