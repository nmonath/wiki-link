package edu.umass.cs.iesl.wikilink.process

import io.Source
import java.io._
import java.util.zip.GZIPInputStream
import edu.umass.cs.iesl.wikilink.google._

object Process {

  var baseOutputDir = ""
  def constructFilePath(i: Int): String = {
    baseOutputDir + "/" + i.toString
  }

  def getSource(file: String, id: Int): Source =
    Source.fromInputStream(new GZIPInputStream(new FileInputStream(constructFilePath(id))))

  def getPage(x: (Webpage, Int)): Unit = {
    val (page, i) = x

    val src = getSource(page.url, i)

    try {

      


    } catch {

    }

  }

  def processPages(pages: WebpageIterator): Unit = {
    pages.zipWithIndex.sliding(100,100).toSeq.par.foreach(chunk => {
      chunk.foreach(processPage(_))
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
