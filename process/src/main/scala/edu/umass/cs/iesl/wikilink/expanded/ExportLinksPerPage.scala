package edu.umass.cs.iesl.wikilink.expanded

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.net.URL
import java.util.zip.GZIPOutputStream

/**
  * Export the page name, mention and entity for each link.
  */
object ExportLinksPerPage {

  def main(args: Array[String]): Unit = {
    val wikiLinksDir = args(0)
    val outFile = args(1)

    val iterator = WikiLinkItemIterator(wikiLinksDir)
    val pw = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outFile)),"UTF-8"))

    iterator.foreach{
      item =>
        val url = item.`url`
        val domainName = url.replaceAllLiterally("http://","").replaceAllLiterally("https://","").split("/").head
        val mentions = item.`mentions`
        val docId = item.`docId`
        mentions.foreach {
          m =>
            val surfaceForm = m.`anchorText`
            val wikiUrl = m.`wikiUrl`
            pw.println(s"$docId\t$url\t$domainName\t$surfaceForm\t$wikiUrl")
        }
    }

    pw.close()


  }



}
