package edu.umass.cs.iesl.wikilink.retrieve

import dispatch._
import java.io._
import org.apache.http.conn.HttpHostConnectException
import java.util.zip.GZIPOutputStream
import edu.umass.cs.iesl.wikilink.google._
import org.apache.http.client.ClientProtocolException
import cc.refectorie.user.sameer.util.CmdLine
import collection.mutable.HashSet

/**
 * @author brian, sameer
 */
object Retrieve {

  var baseOutputDir = ""

  def constructFilePath(page: Webpage): String = {
    var i = page.id
    val firstDir = (i / 1e6).toInt
    val secondDir = ((i % 1e6) / 1e3).toInt
    val name = ((i % 1e3)).toInt
    val dir = "%s/%03d/%03d/".format(baseOutputDir, firstDir, secondDir)
    new File(dir).mkdirs()
    "%s%03d".format(dir, name) + ".gz"
  }

  def getOutputStream(page: Webpage): OutputStream =
    new GZIPOutputStream(new FileOutputStream(constructFilePath(page)))

  def writeError(out: OutputStream, e: String): Unit = {
    out.write(e.getBytes)
  }

  def getPage(page: Webpage, h: Http): Unit = {
    val out = getOutputStream(page)

    if (page.url.contains("http://") || page.url.contains("https://")) {
      try {

        h(url(page.url) >>> out)

      } catch {
        case _: HttpHostConnectException => writeError(out, "!!!HTTP_HOST_CONNECT_EXCEPTION\t" + page.url)
        case _: ClientProtocolException => writeError(out, "!!!CLIENT_PROTOCOL_EXCEPTION\t" + page.url)
        case e: StatusCode => {
          e.code match {
            case 404 => writeError(out, "!!!404\t" + page.url)
          }
        }
        case _: IllegalArgumentException => writeError(out, "!!!ILLEGAL_ARGUMENT_EXCEPTION\t" + page.url)
        case e: Exception => e.printStackTrace(); writeError(out, e.getStackTrace.mkString("\n"))
      }
    }
    else {
      writeError(out, "!!!UNSUPPORTED_PROTOCOL\t" + page.url)
    }

    out.flush()
    out.close()
  }
  

  def downloadUrls(pages: WebpageIterator, resume: Boolean): Unit = {

    lazy val progressFile = { 
      val f = new File(baseOutputDir + "/progress")
      if (!f.exists())
        f.createNewFile()
      f
    }

    lazy val progressWriter = new PrintWriter(new FileWriter(progressFile, true))

    lazy val previouslyDownloaded = HashSet(io.Source.fromFile(progressFile).getLines().map(_.toInt).toSeq: _*)

    def writeCompleteChunkId(i: Int): Unit = {
      progressWriter.synchronized {
        progressWriter.println(i.toString)
        progressWriter.flush()
      }
    }

    // this is a seq of (chunk: Seq(page, pageId), chunkId)
    pages.zipWithIndex.sliding(100, 100).zipWithIndex.toSeq.par.foreach(chunkAndId => {
      val (chunk, chunkId) = chunkAndId
      if (resume && previouslyDownloaded.contains(chunkId))
        () // do nothing
      else {
        val h = new Http
        chunk.foreach(p => getPage(p._1, h))
        h.shutdown()
        if (resume)
          writeCompleteChunkId(chunkId)
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val opts = CmdLine.parse(args)
    println(opts)
    val filename = opts.getOrElse("file", "/Users/sameer/tmp/input")
    val output = opts.getOrElse("output", "/Users/sameer/tmp/output")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val workers = opts.getOrElse("workers", "100").toInt
    val resume = opts.getOrElse("resume", "false").toBoolean

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(workers)

    baseOutputDir = output
    new File(baseOutputDir).mkdirs()

    val iter = new WebpageIterator(filename, takeOnly)
    downloadUrls(iter, resume)
  }

}

