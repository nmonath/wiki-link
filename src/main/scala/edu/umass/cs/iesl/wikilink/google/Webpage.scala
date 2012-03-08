package edu.umass.cs.iesl.wikilink.google

import collection.mutable.ArrayBuffer
import java.io.{BufferedReader, FileReader, Reader, InputStream}
import java.util.regex.Pattern

/**
 * @author sameer
 * @date 3/8/12
 */

case class RareWord(val word: String, val offset: Int)

object RareWord {
  def fromText(rareWordStr: String) = {
    val index = rareWordStr.lastIndexOf(",")
    assert(index != -1)
    new RareWord(rareWordStr.substring(0, index), rareWordStr.substring(index + 1).toInt)
  }
}

case class Mention(val text: String, val offset: Int, val wikiURL: String)

object Mention {

  val matcher = Pattern.compile(",[1-9][0-9]+,(s?http|ftp|file)").matcher("")

  def fromText(mentionStr: String) = {
    var m: Mention = null
    try {
      matcher.reset(mentionStr)
      var start = -1
      var end = -1
      while (matcher.find) {
        start = matcher.start
        end = matcher.end + 1
      }
      assert(start > 0 && end > 0, "start and end not found : " + mentionStr)
      assert(start < mentionStr.length() && end < mentionStr.length(), "start %d and end %d too large: %s (%d)".format(start, end, mentionStr, mentionStr.length()))
      end = mentionStr.substring(0, end - 1).lastIndexOf(",")
      //assert(mentionStr.substring(end - 5).startsWith("http")||mentionStr.substring(end - 5).startsWith("shttp"), "extracted %s from %s".format(mentionStr.substring(end - 5), mentionStr))
      m = new Mention(mentionStr.substring(0, start), mentionStr.substring(start + 1, end).toInt, mentionStr.substring(end + 1))
    } catch {
      case e: Exception => println("Error when extracting from %s".format(mentionStr)); e.printStackTrace(); System.exit(1)
    }
    m
  }
}

class Webpage {
  var url: String = null
  val mentions: ArrayBuffer[Mention] = new ArrayBuffer
  val rareWords: ArrayBuffer[RareWord] = new ArrayBuffer

  override def toString = "url:\t%s\nmentions:\n%s\nwords:\n%s".format(url, mentions.mkString("\t", "\n\t", ""), rareWords.mkString("\t", "\n\t", ""))
}

object Webpage {
  def getNext(stream: BufferedReader): Webpage = {
    val url = stream.readLine
    val mentionsStr = stream.readLine
    val rareWordsStr = stream.readLine
    val mt1 = stream.readLine
    assert(mt1.trim.length == 0)
    val mt2 = stream.readLine
    assert(mt1.trim.length == 0)
    // create the webpage
    val wp = new Webpage
    wp.url = url.trim
    for (mentionStr <- mentionsStr.split("\t")) {
      wp.mentions += Mention fromText mentionStr
    }
    for (rareWordStr <- rareWordsStr.split("\t")) {
      wp.rareWords += RareWord fromText rareWordStr
    }
    wp
  }
}