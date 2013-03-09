package edu.umass.cs.iesl.wikilink

import edu.umass.cs.iesl.wikilink.google.WebpageIterator
import cc.refectorie.user.sameer.util.{TimeUtil, CmdLine}

/**
 * @author sameer
 * @date 10/15/12
 */
object WikiURLDistribution {
  def main(args: Array[String]) {
    val opts = CmdLine.parse(args)
    println(opts)
    val filename = opts.getOrElse("file", "/Users/sameer/tmp/input")
    val output = opts.getOrElse("output", "/Users/sameer/tmp/output")
    TimeUtil.init
    var pageId = 0
    var mentionId: Long = 0
    val wikiMap: Clustering = new WikiURLMap
    for (page <- new WebpageIterator(filename)) {
      if (pageId % 100000 == 0) {
        TimeUtil.snapshot("Done %d".format(pageId))
      }
      for (mention <- page.mentions) {
        mentionId += 1
        wikiMap.addMention(mention, mentionId)
      }
      pageId += 1
    }
    TimeUtil.snapshot(wikiMap.toString)
    wikiMap.writeSizesToFile(output)
  }
}
