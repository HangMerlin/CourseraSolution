package scalashop

import org.scalameter._

object VerticalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 5,
    Key.exec.maxWarmupRuns -> 10,
    Key.exec.benchRuns -> 10,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 1
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime")

    val numTasks = 128
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime")
    println(s"speedup: ${seqtime.value / partime.value}")
  }

}

/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur extends VerticalBoxBlurInterface {

  /** Blurs the columns of the source image `src` into the destination image
   *  `dst`, starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each column, `blur` traverses the pixels by going from top to
   *  bottom.
   */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
      for(x <- from until end; y <- 0 until src.height) {
        dst.update(x, y, boxBlurKernel(src, x, y , radius))
      }
  }

  /** Blurs the columns of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  columns.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
//    val rangeList: Seq[(Int, Int)] = {
//      val intervals = Range(0, src.width, numTasks)
//      if (src.width % numTasks == 0)intervals.dropRight(1).zip(intervals.tail :+ src.width)
//      else intervals.zip(intervals.tail :+ src.width)
//    }

    val columns = 0 until src.width by Math.ceil(src.width.toDouble / numTasks).toInt
    val rangeList = (columns zip columns.tail) :+ (columns.last, src.width)

    val tasks = rangeList.map(range => task(blur(src, dst, range._1, range._2, radius)))
    tasks.foreach(_.join)

  }

}
