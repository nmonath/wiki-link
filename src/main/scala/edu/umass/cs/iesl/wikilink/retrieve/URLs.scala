package edu.umass.cs.iesl.wikilink.retrieve

import dispatch._
import java.io._
import org.apache.http.conn.HttpHostConnectException
import java.util.zip.GZIPOutputStream
import edu.umass.cs.iesl.wikilink.google._
import org.apache.http.client.ClientProtocolException

object Retrieve {

  var baseOutputDir = ""
  def constructFilePath(i: Int): String = {
    baseOutputDir + "/" + i.toString
  }

  def getOutputStream(file: String, id: Int): OutputStream =
    new GZIPOutputStream(new FileOutputStream(constructFilePath(id)))

  def writeError(out: OutputStream, e: String): Unit = { out.write(e.getBytes) }

  def getPage(x: (Webpage, Int), h: Http): Unit = {
    val (page, i) = x

    val out = getOutputStream(page.url, i)

    if (page.url.contains("http://") || page.url.contains ("https://")) {
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
      }
    }
    else {
      writeError(out, "!!!UNSUPPORTED_PROTOCOL\t" + page.url)
    }

    out.flush()
    out.close()
  }

  def downloadUrls(pages: WebpageIterator): Unit = {
    pages.zipWithIndex.sliding(100,100).toSeq.par.foreach(chunk => {
      val h = new Http
      chunk.foreach(getPage(_, h))
      h.shutdown()
    })
  }

  def main(args : Array[String]): Unit = {
    // arg0: data file
    // arg1: output directory
    // arg2: number of workers

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(args(2).toInt)

    baseOutputDir = args(1)
    val iter = new WebpageIterator(args(0), takeOnly = 10000)
    downloadUrls(iter)
  }

}

