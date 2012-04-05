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

  val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())

  // print the wikipedia links by first projecting to <p> then to <a>
  def processPage(page: Webpage): Unit = {
    try {
      val contentStream = getPageContentStream(page)
      val ns = parser.load(contentStream)

      (ns \\ "p").foreach { p =>
        (p \\ "a").map(a => (a.text, a.attribute("href"))).filter({
          case (_, Some(href)) => href.text.contains("wikipedia.org")
          case (_, None)       => false
        }).foreach { case (anchorText, href) => println("%s (%d, %d) --> %s %s".format(href.get.text, 0, 0, anchorText, p.text)) }
      }

    } catch { case _ => () }
  }

  def main(args : Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    args.foreach(println(_))
    val dataFile = opts.getOrElse("dataset", "")
    val pagesDir = opts.getOrElse("pages", "/Users/sameer/tmp/input")
    val output = opts.getOrElse("output", "/Users/sameer/tmp/process-output")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val workers = opts.getOrElse("workers", "100").toInt
    val resume = opts.getOrElse("resume", "false").toBoolean

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(workers)

    baseContentDir = pagesDir
    val pages = new WebpageIterator(dataFile, takeOnly = takeOnly)
    for (page <- pages)
      processPage(page)
  }

}
