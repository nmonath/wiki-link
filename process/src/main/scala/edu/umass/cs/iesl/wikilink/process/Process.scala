package edu.umass.cs.iesl.wikilink.process

/*
 * Process downloaded HTML files.
 *
 * @author Brian Martin
 * @date 4/5/2012
 */

import java.io._
import xml.XML
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import edu.umass.cs.iesl.wikilink.google._
import cc.refectorie.user.sameer.util.CmdLine
import net.liftweb.json._
import net.liftweb.json.Serialization.{read, write}
import collection.mutable.ArrayBuffer
import collection.immutable.HashSet

object PageSerialization {
  case class Context(left: String,  right: String)
  case class PageWrapper(page: Webpage, contexts: Seq[Context])
}

object Process {
  import PageSerialization._

  var baseContentDir = ""
  def getPageContentStream(page: Webpage): InputStream =
    new FileInputStream("%s/%06d/%d".format(baseContentDir, page.id / 1000, page.id % 1000))

  val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())

  implicit val formats = Serialization.formats(NoTypeHints) //ShortTypeHints(List(classOf[Mention], classOf[RareWord])))

  // print the wikipedia links by first projecting to <p> then to <a>
  def processPage(page: Webpage): Unit = {

    try {
      val contentStream = getPageContentStream(page)
      val ns = parser.load(contentStream)
      val mentionUrls = HashSet[String](page.mentions.map(_.wikiURL): _*)
      val contexts = ArrayBuffer[Context]()

      (ns \\ "p").foreach { p =>
        (p \\ "a").
          map( a => (a.text, a.attribute("href"))).
          filter({
            case (_, Some(href)) => { href.text.contains("wikipedia.org") && mentionUrls.contains(href.text) }
            case (_, None)       => false }).
          foreach {
            case (text, href) => {
              val anchorOffset = p.text.indexOf(text)
              val left = p.text.substring(0, anchorOffset)
              val right = p.text.substring(anchorOffset + text.length)
              contexts append new Context(left, right)
            }
          }
        }

      println(write(new PageWrapper(page, contexts)))
      
    } catch { case _ => () }
  }

  def main(args : Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    args.foreach(println(_))
    val dataFile = opts.getOrElse("dataset", "../retrieve/wikilink-dataset.dat")
    val pagesDir = opts.getOrElse("pages", "../retrieve/pages")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt

    baseContentDir = pagesDir
    val pages = new WebpageIterator(dataFile, takeOnly = takeOnly)
    for (page <- pages)
      processPage(page)
  }

}
