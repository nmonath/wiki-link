package edu.umass.cs.iesl.wikilink.process

import cc.refectorie.user.sameer.util.CmdLine
import edu.umass.cs.iesl.wikilink.google
import edu.umass.cs.iesl.wikilink.google.{Webpage, WebpageIterator, RareWord}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits.parseInt
import net.liftweb.json._
import net.liftweb.json.Serialization.{write, read}
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import xml.factory.XMLLoader
import xml.{Elem, XML}
import java.io.{File, FileInputStream, PrintWriter}
import collection.mutable.{HashMap, ArrayBuffer, HashSet}
import java.util.zip.GZIPInputStream

/**
 * Author: martin
 * Date: 4/8/12
 */

object ProcessedPageFormat {
  case class ExtractedContext(text: String, left: String,  right: String, fromParser: Boolean = true) { def full: String = left + text + right }
  case class GoogleMentionAnnotations(text: String, offset: Int)
  case class Mention(url: String, extractedContext: ExtractedContext, googleAnnnotations: GoogleMentionAnnotations)
  case class GooglePageAnnotations(rareWords: Seq[RareWord])
  case class Page(id: Int, url: String, mentions: Seq[Mention], googleAnnotations: GooglePageAnnotations)
  case class PagesChunk(pages: Seq[Page])
}

object Unjsonify {
  def apply[T <: AnyRef](s: String)(implicit m: Manifest[T]): T = read[T](s)
  implicit val formats = Serialization.formats(NoTypeHints)
}

object Jsonify {
  def apply[T <: AnyRef](t: T): String = {
    val res = write(t)
    println(res)
    res
  }
  implicit val formats = Serialization.formats(NoTypeHints)
}

object Process {

  // keys
  val ORIG_PAGES         = "orig-pages"
  val ORIG_PAGES_MAX_IDX = "orig-pages-max-idx"
  val CHUNK_LIST         = "chunk-list"
  val FAILED_CHUNK_LIST  = "failed-chunk-list"
  val CHUNK_SIZE         = "chunk-length"
  val KILL               = "kill"

  val CONTEXT_RADIUS     = 300

  var pagesDir = ""

  def main(args: Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    val dataFile = opts.getOrElse("dataset", "../retrieve/wikilink-dataset.dat")
    val _pagesDir = opts.getOrElse("pages", "../retrieve/pages")
    val jsonPath = opts.getOrElse("json", "../pages.json")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val chunkSize = opts.getOrElse("chunk", 1000.toString).toInt
    val redisAddr = opts.getOrElse("redis", "localhost:6379")
    val role      = opts.getOrElse("role", "slave")

    pagesDir = _pagesDir

    val (host, port) = redisAddr.splitAt(redisAddr.indexOf(":"))
    val redis = new RedisClient(host, port.drop(1).toInt)

    role match {
      case "master" => {
        redis.del(ORIG_PAGES)
        redis.del(ORIG_PAGES_MAX_IDX)
        val pages = new WebpageIterator(dataFile, takeOnly = takeOnly)
        val numWritten = populateRedisWithPages(redis, pages)
        populateRedisWithChunks(redis, chunkSize, maxIdx = numWritten)
      }
      case "slave" => {
        val chunkSize = redis.get[Int](CHUNK_SIZE).get
        val maxIdx = redis.get[Int](ORIG_PAGES_MAX_IDX).get
        while (!redis.exists(KILL)) {
          redis.lpop[Int](CHUNK_LIST) match {
            case None => System.exit(0) // no more chunks
            case Some(chunkIdx) => {
              try {
                processChunk(chunkIdx, chunkSize, maxIdx, jsonPath, pagesDir, redis)
              }
              catch {
                case e => {
                  redis.lpush(FAILED_CHUNK_LIST, chunkIdx)
                  e.printStackTrace()
                  System.exit(0)
                }
              }
            }
          }
        }
      }
    }
  }
  
  def populateRedisWithPages(redis: RedisClient, pages: WebpageIterator, keyPrefix: String = "page"): Int = {
    var i = -1
    for (page <- pages) {
      redis.hset(ORIG_PAGES, page.id, Jsonify(page))
      i += 1
    }
    redis.set(ORIG_PAGES_MAX_IDX, i)
    i
  }
  
  def populateRedisWithChunks(redis: RedisClient, chunkSize: Int, maxIdx: Int) {
    redis.set(CHUNK_SIZE, chunkSize)

    val numChunks = {
      val fullChunks = (maxIdx / chunkSize)
      fullChunks + { if (maxIdx % chunkSize > 0) 1 else 0 }
    }
    
    for (i <- 0 until numChunks)
      redis.rpush(CHUNK_LIST, i)
  }

  def processChunk(chunkIdx: Int, chunkSize: Int, maxIdx: Int, jsonPath: String, pagesDir: String, redis: RedisClient) {
    val writer = new PrintWriter(jsonPath + "/%06d.json".format(chunkIdx))
    val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())
    val startIdx = chunkIdx * chunkSize
    val endIdx   = math.min(startIdx + chunkSize, maxIdx)
    for (i <- startIdx until endIdx) {
      val page = Unjsonify[Webpage](redis.hget[String](ORIG_PAGES, i).get)
      val jsonStr = processPage(page, parser)
      if (jsonStr != "")
        writer.println(jsonStr)
    }
    writer.close()
  }

  def processPage(page: Webpage, parser: XMLLoader[Elem]): String = {
    import ProcessedPageFormat._

    def getPath(i: Int) = "/%06d/%d".format(i / 1000, i % 1000) + ".gz"
    def getContentStream = new GZIPInputStream(new FileInputStream(pagesDir + getPath(page.id)))

    // pruning is necessary because sites might garbled urls like: <a href="//en.wikipedia...."> (that is, no "http:")
    def pruneUrl(url: String): String = url.substring(url.indexOf("wikipedia.org"))
    // track a mapping from full url's to the original mention (so we can add the "GoogleAnnotations" when creating mentions)
    val prunedToMention = HashMap[String, google.Mention](page.mentions.map(m => (pruneUrl(m.url), m)): _*)

    val csP = getContentStream
    val csDiv = getContentStream

    val res = try {
      var ns = parser.load(csP)

      val prunedMentionUrls = HashSet[String](page.mentions.map(m => pruneUrl(m.url)): _*)
      val mentions = ArrayBuffer[Mention]()
      val coveredUrls = new HashSet[String]

      // TODO: maybe the following two loops and contentStreams could be merged.

      // first attempt to find the links and use paragraphs as the context
      (ns \\ "p").foreach { p =>
        (p \\ "a").
          map( a => (a.text, a.attribute("href"))).
          filter({
            case (_, Some(href)) => { href.text.contains("wikipedia.org") && prunedMentionUrls.contains(pruneUrl(href.text)) }
            case (_, None)       => false }).
          foreach {
            case (text, href) => {
              val anchorOffset = p.text.indexOf(text)
              val left = p.text.substring(0, anchorOffset)
              val right = p.text.substring(anchorOffset + text.length)
              val prunedUrl = pruneUrl(href.get.text)
              val originalMention = prunedToMention(prunedUrl)
              if (!coveredUrls.contains(prunedUrl)) {
                coveredUrls += prunedUrl
                mentions append new Mention(
                  href.get.text,
                  new ExtractedContext(text, left, right),
                  new GoogleMentionAnnotations(originalMention.text, originalMention.offset)
                )
              }
          }
        }
      }

      // next attempt the same thing, but with div's not paragraphs
      // (and chop off some of the context)
      if (mentions.size != prunedMentionUrls.size) {
        ns = parser.load(csDiv)
        (ns \\ "div").reverse.foreach { div => // the reverse here encourages inner div's first
          (div \\ "a").
            map( a => (a.text, a.attribute("href"))).
            filter({
              case (_, Some(href)) => { href.text.contains("wikipedia.org") && prunedMentionUrls.contains(pruneUrl(href.text)) }
              case (_, None)       => false }).
            foreach {
              case (text, href) => {
                val anchorOffset = div.text.indexOf(text)
                val left = div.text.substring(math.max(0,anchorOffset - CONTEXT_RADIUS), anchorOffset)
                val right = div.text.substring(anchorOffset + text.length, math.min(anchorOffset + text.length + CONTEXT_RADIUS, div.text.length))
                val prunedUrl = pruneUrl(href.get.text)
                val originalMention = prunedToMention(prunedUrl)
                if (!coveredUrls.contains(prunedUrl)) {
                  coveredUrls += prunedUrl
                  mentions append new Mention(
                    href.get.text,
                    new ExtractedContext(text, left, right),
                    new GoogleMentionAnnotations(originalMention.text, originalMention.offset)
                  )
                }
              }
            }
        }
      }

      // alternative if parsing the html didn't find the mentions
      // the idea is simply to:
      //   1. read the whole file in as a string
      //   2. match the url
      //   3. find the anchor text by matching the '>' of the <a> and the "</a>"
      //   4. get a left context from before the url, and a right context from after the anchor text
      if (mentions.size != prunedMentionUrls.size) {
        val cs2 = getContentStream
        val string = io.Source.fromInputStream(cs2).getLines().mkString("\n"); cs2.close()
        for ((prunedUrl, originalMention) <- prunedToMention if (!coveredUrls.contains(originalMention.url))) {
          coveredUrls += prunedUrl
          val unprunedUrl = originalMention.url
          val urlIdx = string.indexOf(unprunedUrl)
          if (urlIdx > -1) {
            val beginAnchorText: Int = {
              ">".r.findFirstMatchIn(string.drop(urlIdx)) match {
                case Some(m) => m.end
                case None => 0 // ???
              }
            }
            val endAnchorText: Int = {
              "</a>".r.findFirstMatchIn(string.drop(urlIdx)) match {
                case Some(m) => m.start
                case None => 0 // ???
              }
            }
            val anchorText  = string.substring(beginAnchorText, endAnchorText)
            val leftContext = string.substring(math.max(0, urlIdx - CONTEXT_RADIUS), urlIdx)
            val rightContext = string.substring(endAnchorText, math.min(urlIdx + CONTEXT_RADIUS, string.length))
            val originalMention = prunedToMention(prunedUrl)
            mentions append new Mention(
              unprunedUrl,
              new ExtractedContext(anchorText, leftContext, rightContext, fromParser = false),
              new GoogleMentionAnnotations(originalMention.text, originalMention.offset)
            )
          }
        }
      }

      // add with blank extractedContent if none of the above worked
      if (mentions.size != prunedMentionUrls.size) {
        for ((prunedUrl, originalMention) <- prunedToMention if (!coveredUrls.contains(originalMention.url))) {
          mentions append new Mention(
            originalMention.url,
            new ExtractedContext("", "", ""),
            new GoogleMentionAnnotations(originalMention.text, originalMention.offset)
          )
        }
      }

      Jsonify(new Page(page.id, page.url, mentions, new GooglePageAnnotations(page.rareWords)))

    } catch { case _ => "" }

    csP.close()
    csDiv.close()
    res
  }

}

trait ProcessJson {
  import ProcessedPageFormat._

  val name: String
  def processPage(p: Page): String
  def aggregatePages(ss: Seq[String]): String
  def aggregateChunks(ss: Seq[String]): String
  
  lazy val JSON_LIST        = name + "-json-list"
  lazy val RESULT_LIST      = name + "-json-result-list"
  lazy val FAILED_JSON_LIST = name + "-failed-json-list"
  lazy val RUNNING_WORKERS  = name + "-workers"

  def main(args: Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    val jsonPath = opts.getOrElse("json", "../pages.json")
    val outputFile = opts.getOrElse("output", name + ".out")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val redisAddr = opts.getOrElse("redis", "localhost:6379")
    val role      = opts.getOrElse("role", "master")

    val (host, port) = redisAddr.splitAt(redisAddr.indexOf(":"))
    val redis = new RedisClient(host, port.drop(1).toInt)

    role match {
      case "master" => { 
        redis.del(JSON_LIST)
        redis.del(RESULT_LIST)
        redis.del(FAILED_JSON_LIST)
        redis.del(RUNNING_WORKERS)
        populateRedisWithJsonFilePaths(redis, jsonPath, takeOnly) 
      }
      case "slave" => {
        redis.lpush(RUNNING_WORKERS, 1)
        while (redis.exists(RUNNING_WORKERS)) { // running_workers does not exist when a master has been re-run
          redis.lpop[String](JSON_LIST) match {
            case None => {
              if (redis.llen(RUNNING_WORKERS).get == 1) {
                println("writing file")
                val writer = new PrintWriter(outputFile)
                val strings = redis.lrange[String](RESULT_LIST, 0, redis.llen(RESULT_LIST).get).get.map(_.get).toArray.toSeq // redis returns an option of options
                writer.println(aggregateChunks(strings))
                writer.close()
                redis.del(RESULT_LIST)
              }
              redis.lpop[Int](RUNNING_WORKERS)
              System.exit(0)
            }
            case Some(jsonFilePath) => {
              try {
                val pages = Unjsonify[PagesChunk]("""{"pages":[""" + io.Source.fromFile(jsonFilePath).getLines.mkString(",") + "]}")
                val aggregate = aggregatePages(pages.pages.map(processPage(_)))
                redis.rpush(RESULT_LIST, aggregate)
              }
              catch {
                case e => {
                  redis.lpush(FAILED_JSON_LIST, jsonFilePath)
                  println(e.getMessage)
                  println(e.getStackTraceString)
                }
              }
            }
          }
        }
      }
    }
  }

  def populateRedisWithJsonFilePaths(redis: RedisClient, jsonDir: String, maxIdx: Int) {
    val f = new File(jsonDir)
    assert(f.isDirectory)
    var i = 0
    for (jsonFile <- f.listFiles()) {
      if (i > maxIdx) return
      redis.rpush(JSON_LIST, jsonFile.getAbsolutePath)
      i += 1
    }
  }

}


object AggregateToBins {

  case class Bin(bins: Map[String, Int])
  case class Binnable(binIdx: Int,  num: Int)

  private def newHMap = new HashMap[String,Int] { override def default(k: String) = 0 }

  def makeBin(xs: Seq[Binnable]): Bin = {
    val hmap = newHMap
    xs.foreach(b => hmap(b.binIdx.toString) += b.num)
    new Bin(hmap.toMap)
  }

  private def mergeHashMaps(a: HashMap[String,Int], b: Map[String,Int]): HashMap[String,Int] = {
    b.foreach({ case (k,v) => a(k) += v})
    a
  }

  def mergeBins(xs: Seq[Bin]): Bin = new Bin(xs.map(_.bins).foldLeft(newHMap)(mergeHashMaps(_, _)).toMap)

}

trait DefaultBinAggregation {
  import AggregateToBins.{Bin, Binnable}
  def aggregatePages(ss: Seq[String]): String = Jsonify(AggregateToBins.makeBin(ss.map(Unjsonify[Binnable](_))))
  def aggregateChunks(ss: Seq[String]): String = Jsonify(AggregateToBins.mergeBins(ss.map(Unjsonify[Bin](_))))
}


import ProcessedPageFormat._
import AggregateToBins.{Binnable, Bin}

object AverageContextSize extends ProcessJson with DefaultBinAggregation {
  val name = "avg-context-size"
  def processPage(page: Page): String =
    Jsonify( new Binnable( page.mentions.map(_.extractedContext.full.length).sum, page.mentions.length ))
}

object NumPagesWithMentions extends ProcessJson with DefaultBinAggregation {
  val name = "num-pages-with-mentions"
  def processPage(page: Page): String = Jsonify(new Binnable(page.mentions.size, 1))
}

object ContextWordCount extends ProcessJson {
  val name = "context-word-count"
  def processPage(page: Page): String = Jsonify(
    new Bin( HashMap[String, Int](page.mentions.flatMap(m => "\\s+".r.split(m.extractedContext.full)).map((_, 1)): _*).toMap ) )
  
  def aggregatePages(ss: Seq[String]): String = aggregateChunks(ss)
  def aggregateChunks(ss: Seq[String]): String = Jsonify(AggregateToBins.mergeBins(ss.map(Unjsonify[Bin](_))))
}


object ParserTest {
  def main(args: Array[String]): Unit = {
    val parser = XML.withSAXParser((new SAXFactoryImpl).newSAXParser())
    val string = """
      <div id="outer">
        <div id="inner1">
          <a href="http://en.wikipedia.org/wiki/College_Board" title="College Board">
            <span style="color: windowtext; text-decoration: none; text-underline: none;">
              College Board
            </span>
          </a>
        </div>
        <div id="inner2">
          <a> inner2 link </a>
        </div>
      </div>
      <div id="last">
        <a> last link </a>
      </div>
    """
    
    val x = parser.loadString(string)

    for (n <- x.child.iterator) {
      println("----")
      println(n)
    }
    
//    (x \\ "div").reverse.foreach ( div => {
//      (div \\ "a").foreach(a => {
//        println("div.id: " + (div attribute "id"))
//        println("a.text: " + a.text)
//      })
//    })

  }
}
