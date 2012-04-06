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
import edu.umass.cs.iesl.wikilink.google.{Webpage, WebpageIterator, RareWord}
import cc.refectorie.user.sameer.util.CmdLine
import net.liftweb.json._
import net.liftweb.json.Serialization.{write => writeJson, read => readJson}
import collection.mutable.ArrayBuffer
import collection.immutable.HashSet

object PageSerialization {
  var baseJsonDir = ""

  case class Context(left: String,  right: String)
  case class Mention(text: String, url: String, context: Context)
  case class Page(id: Int, mentions: Seq[Mention], rareWords: Seq[RareWord])

  def getPath(i: Int) = "/%06d/%d".format(i / 1000, i % 1000)

  def writeToFile(page: Page): Unit = {
    // make the enlosing directory
    val dir = new File(baseJsonDir + "/%06d".format(page.id / 1000))
    if (!dir.exists()) dir.mkdirs()

    // write the file
    val out = new FileOutputStream(baseJsonDir + getPath(page.id))
    out.write(writeJson(page).getBytes)
    out.close()
    println("Wrote page to : " + baseJsonDir + getPath(page.id))
  }

  def getJsonString(id: Int): Option[String] = {
    val f = new File(baseJsonDir + getPath(id))
    if (f.exists())
      Some(io.Source.fromFile(f).mkString)
    else
      None
  }

  implicit val formats = Serialization.formats(NoTypeHints)
}

object WriteToJSON {
  import PageSerialization._

  var baseContentDir = ""
  def getPageContentStream(page: Webpage): InputStream =
    new FileInputStream(baseContentDir + getPath(page.id))

  val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())

  // print the wikipedia links by first projecting to <p> then to <a>
  def processPage(page: Webpage): Unit = {

    try {
      val contentStream = getPageContentStream(page)
      val ns = parser.load(contentStream)
      val mentionUrls = HashSet[String](page.mentions.map(_.wikiURL): _*)
      val mentions = ArrayBuffer[Mention]()

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
              mentions append new Mention(text, href.get.text, new Context(left, right))
            }
          }
        }

      if (mentions.length > 0) {
        //println(writeJson(new Page(page.id, mentions, page.rareWords)))
        writeToFile(new Page(page.id, mentions, page.rareWords))
      }

    } catch { case _ => () }
  }

  def main(args : Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    args.foreach(println(_))
    val dataFile = opts.getOrElse("dataset", "../retrieve/wikilink-dataset.dat")
    val pagesDir = opts.getOrElse("pages", "../retrieve/pages")
    val jsonDir = opts.getOrElse("json", "../json")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt

    val json = new File(jsonDir)
    if (!json.exists())
      json.mkdirs()

    baseContentDir = pagesDir
    baseJsonDir = jsonDir
    val pages = new WebpageIterator(dataFile, takeOnly = takeOnly)
    for (page <- pages)
      processPage(page)
  }

}

object AverageContextSize {
  import PageSerialization._

  def process(numToProcess: Int): Double = {
    var totalLength = 0
    var totalMentions = 0

    for (i <- 0 to numToProcess) {
      getJsonString(i) match {
        case None       =>
        case Some(json) => {
          val page = readJson[Page](json)
          totalMentions += page.mentions.length
          for (mention <- page.mentions)
            totalLength += mention.context.left.length() + mention.context.right.length + mention.text.length
        }
      }
    }

    totalLength * 1.0 / totalMentions
  }

  def main(args : Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    args.foreach(println(_))
    val jsonDir = opts.getOrElse("json", "../json")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    
    baseJsonDir = jsonDir
    println(process(takeOnly))
  }

}
