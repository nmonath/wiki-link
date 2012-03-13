package edu.umass.cs.iesl.wikilink.process

import io.Source
import java.io._
import java.util.zip.GZIPInputStream
import edu.umass.cs.iesl.wikilink.google._
import edu.umass.cs.iesl.wikilink.retrieve.FilePath
import cc.refectorie.user.sameer.util.CmdLine
import xml.XML
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl

object Process {
  import FilePath._

  def getSource(file: String, page: Webpage): Source =
    Source.fromInputStream(new GZIPInputStream(new FileInputStream(constructFilePath(page))))

  def processPage(x: (Webpage, Int)): Unit = {
    val (page, i) = x

    val src = getSource(page.url, page)

    val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())

    try {

      val ns = parser.loadFile(constructFilePath(page))
      ns \\ "a"


    } catch {
      case _ => println("oops")
    }

  }

  def processPages(pages: WebpageIterator): Unit = {
    pages.zipWithIndex.sliding(100,100).toSeq.par.foreach(chunk => {
      chunk.foreach(processPage(_))
    })
  }

  def main(args : Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    val input = opts.getOrElse("dir", "/Users/sameer/tmp/input")
    val output = opts.getOrElse("process-output", "/Users/sameer/tmp/process-output")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val workers = opts.getOrElse("workers", "100").toInt
    val resume = opts.getOrElse("resume", "false").toBoolean

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(workers)

    baseOutputDir = args(1)
    val iter = new WebpageIterator(args(0), takeOnly = 10000)
    processPages(iter)
  }

}
