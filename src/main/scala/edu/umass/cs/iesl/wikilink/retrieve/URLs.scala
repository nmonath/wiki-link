package edu.umass.cs.iesl.wikilink.retrieve

import dispatch._
import org.apache.http.conn.HttpHostConnectException
import java.util.zip.GZIPOutputStream
import java.io._
import edu.umass.cs.iesl.wikilink.google._
import org.apache.http.client.ClientProtocolException

// TODO: logging
object Retrieve {

  val h = new Http

  def constructFilePath(i: Int): String = "TODO"

  def getOutputStream(file: String, id: Int): OutputStream =
    new GZIPOutputStream(new FileOutputStream(constructFilePath(id)))


  def downloadUrls(pages: WebpageIterator): Unit = {
    pages.foreach(page => {
      val out = getOutputStream(page.url, 0)
      try {
        h(url(page.url) >>> out)
      } catch {
        // TODO: write files for errors
        case _: HttpHostConnectException => println("skipping, host conn: " + page.url)
        case _: ClientProtocolException => println("skipping, host conn: " + page.url)
        case e: StatusCode => {
          e.code match {
            case 404 => println("skipping, 404 error: " + page.url)
          }
        }
      }
      println("DONE: " + page.url)

    })

  }

  def main(args : Array[String]): Unit = {
    downloadUrls(new WebpageIterator(args(0), takeOnly = 10))
  }

}