package edu.umass.cs.iesl.wikilink.retrieve

import java.io._
import org.apache.commons.net.ftp.{FTPClient, FTPReply}

// not yet working
object FTP {

  val ftp = new FTPClient()

  def retrieveUrlAsInputStream(host: String, filePath: String): InputStream = {

    var error = false
    var stream: InputStream = null
    try {
      // connect
      ftp.connect(host)
      println("Connected to " + host + ".")
      print(ftp.getReplyString)

      // verify success
      val reply = ftp.getReplyCode
      if(FTPReply.isPositiveCompletion(reply))
        stream = ftp.retrieveFileStream(filePath.drop(1))
      else
        println("FTP server refused connection.")

      ftp.logout()
    } catch {
      case e: IOException => {
        error = true;
        e.printStackTrace();
      }
    }

    if(ftp.isConnected)
      try { ftp.disconnect() } catch { case _: IOException => () }

    return stream
  }
}
