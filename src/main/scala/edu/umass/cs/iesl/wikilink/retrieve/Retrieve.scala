package edu.umass.cs.iesl.wikilink.retrieve

import dispatch._
import java.io._
import java.net.{UnknownHostException, SocketException}
import org.apache.http.conn.HttpHostConnectException
import org.apache.commons.io.IOUtils
import java.util.zip.GZIPOutputStream
import edu.umass.cs.iesl.wikilink.google._
import org.apache.http.client.ClientProtocolException
import cc.refectorie.user.sameer.util.CmdLine
import collection.mutable.{HashSet, HashMap}
import actors.Actor
import actors.Actor._
import actors.Futures.future

/**
 * @author brian, sameer
 */

object FilePath {
  var baseOutputDir = ""

  def constructDirectoryPath(page: Webpage): String = {
    var i = page.id
    val firstDir = (i / 1e6).toInt
    val secondDir = ((i % 1e6) / 1e3).toInt
    val dir = "%s/%03d/%03d/".format(baseOutputDir, firstDir, secondDir)
    new File(dir).mkdirs()
    dir
  }

  def constructFileName(page: Webpage): String =
    "%03d".format((page.id % 1e3).toInt)

  def constructFilePath(page: Webpage): String =
    constructDirectoryPath(page) + constructFileName(page)
}

object Retrieve {
  import FilePath._

  def getOutputStream(page: Webpage): OutputStream =
    new GZIPOutputStream(new FileOutputStream(constructFilePath(page)))

  def writeError(out: OutputStream, e: String): Unit = {
    out.write(e.getBytes)
  }

  // could put an actor here
  val contentWriters = new HashMap[String, FileWriter]
  def writeContentType(page: Webpage, contentType: String): Unit = {
    contentWriters.synchronized {
      val outputDir = constructDirectoryPath(page)
      val out = contentWriters.getOrElseUpdate(
        outputDir,
        { val f = new File(outputDir + "/content")
          if (!f.exists())
            f.createNewFile()
          new FileWriter(f, true) // append = true
        })
      out.append(constructFileName(page) + "\t" + contentType + "\n")
      out.flush()
    }
  }
  
  def getPage(page: Webpage, http: Http): Unit = {
    val out = getOutputStream(page)
    println(constructFilePath(page))

    if (page.url.contains("http://") || page.url.contains("https://")) {
      try {
        val contentType: Option[String] =
          http(url(page.url) >+ { r =>
            (r >:> { h => h("Content-Type").headOption },
             r >> { in => out.write(IOUtils.toByteArray(in)) })
          })._1
        
        if (contentType == None)
          writeContentType(page, "None")
        else
          writeContentType(page, contentType.get)

      } catch {
        case _: HttpHostConnectException => writeError(out, "!!!HTTP_HOST_CONNECT_EXCEPTION\t" + page.url)
        case _: ClientProtocolException => writeError(out, "!!!CLIENT_PROTOCOL_EXCEPTION\t" + page.url)
        case _: UnknownHostException => writeError(out, "!!!UNKOWN_HOST_EXCEPTION\t" + page.url)
        case _: SocketException => writeError(out, "!!!SOCKET_EXCEPTION\t" + page.url)
        case e: StatusCode => {
          if (e.code == 404)
            writeError(out, "!!!STATUS_CODE_404\t" + page.url)
          else
            writeError(out, "!!!STATUS_CODE_"+e+"\t" + page.url)
        }
        case _: IllegalArgumentException => writeError(out, "!!!ILLEGAL_ARGUMENT_EXCEPTION\t" + page.url)
        case e: Exception => { e.printStackTrace(); writeError(out, page.url + "\n" + e.getStackTrace.mkString("\n")) }
        case _ => { writeError(out, "!!!UNIDENTIFIED_ERROR\t" + page.url)}
      }
    }
    else {
      writeError(out, "!!!UNSUPPORTED_PROTOCOL\t" + page.url)
    }

    out.flush()
    out.close()
  }
  

  // resume is actually trackProgress
  def downloadUrls(pages: WebpageIterator, workers: Int, resume: Boolean): Unit = {

    lazy val progressFile = { 
      val f = new File(baseOutputDir + "/progress")
      if (!f.exists())
        f.createNewFile()
      f
    }

    case class WriteInt(i: Int)
    val progressWriter = new PrintWriter(new FileWriter(progressFile, true))
    val progressActor: Actor = actor { loop { react { case WriteInt(i) => progressWriter.write(i.toString + "\n") } } }

    lazy val previouslyDownloaded = HashSet(io.Source.fromFile(progressFile).getLines().map(_.toInt).toSeq: _*)
    
    case class Next()
    var chunkId = 0
    val iteratorActor = actor {
      loop {
        react {
          case Next() => {
            reply((chunkId, pages.take(100)))
            chunkId += 1
          }
        }
      }
    }

    def workerDownloadLoop() {
      val h = new Http
      while (true) {
        val (chunkId, pages) = (iteratorActor !? Next()).asInstanceOf[(Int, Iterator[Webpage])]

        if (pages.isEmpty) {
          h.shutdown()
          return
        }

        for (page <- pages) {
          try {
            getPage(page, h)
          }
          catch {
            case _ => println("!!!unknown failure on " + page.id)
          }
        }

        if (resume)
          progressActor ! WriteInt(chunkId)
      }
    }

    val fs = (1 to workers).map{ i => future { workerDownloadLoop() } }
    fs.foreach { f => f() }
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
    downloadUrls(iter, workers, resume)
  }

}

