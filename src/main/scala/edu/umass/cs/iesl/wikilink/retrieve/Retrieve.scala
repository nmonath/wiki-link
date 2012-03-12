package edu.umass.cs.iesl.wikilink.retrieve

import dispatch._
import java.io._
import org.apache.http.conn.HttpHostConnectException
import java.util.zip.GZIPOutputStream
import edu.umass.cs.iesl.wikilink.google._
import org.apache.http.client.ClientProtocolException
import cc.refectorie.user.sameer.util.CmdLine

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
    "%s%03d".format(dir, name)
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

  def downloadUrls(pages: WebpageIterator): Unit = {
    pages.zipWithIndex.sliding(100, 100).toSeq.par.foreach(chunk => {
      val h = new Http
      chunk.foreach(p => getPage(p._1, h))
      h.shutdown()
    })
  }

  def main(args: Array[String]): Unit = {
    // arg0: data file
    // arg1: output directory
    // arg2: number of workers
    val opts = CmdLine.parse(args)
    println(opts)
    val filename = opts.getOrElse("file", "/Users/sameer/tmp/input")
    val output = opts.getOrElse("output", "/Users/sameer/tmp/output")
    val takeOnly = opts.getOrElse("take", Int.MaxValue.toString).toInt
    val workers = opts.getOrElse("workers", "100").toInt

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(workers)

    baseOutputDir = output
    val iter = new WebpageIterator(filename, takeOnly)
    downloadUrls(iter)
  }

}

