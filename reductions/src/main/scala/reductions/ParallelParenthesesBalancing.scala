package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def balanceWithCount(chars: Array[Char], count: Int): Boolean = {
      if (chars.isEmpty) count match {
        case 0 => true
        case _ => false
      } else chars.head match {
        case '(' => balanceWithCount(chars.tail, count + 1)
        case ')' => if (count > 0) balanceWithCount(chars.tail, count - 1) else false
        case _ => balanceWithCount(chars.tail, count)
      }

    }
    balanceWithCount(chars, 0)
  }

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(from: Int, until: Int, counterLeft: Int, counterRight: Int): (Int, Int) = {
      if (until >= from){
        chars(from) match {
          case '('                    => traverse(from + 1, until, counterLeft + 1, counterRight)
          case ')' if counterLeft > 0 => traverse(from + 1, until, counterLeft - 1, counterRight)
          case ')'                    => traverse(from + 1, until, counterLeft, counterRight + 1)
          case _                      => traverse(from + 1, until, counterLeft, counterRight)
        }
      } else (counterLeft, counterRight)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = from + ( until - from ) / 2
        val ((a1, a2), (b1, b2)) = parallel(reduce(from, mid), reduce(mid, until))
        if (a1 > b2)
          (a1 - b2 + b1 , a2)
        else
          (b1, b2 - a1 + a2)
      }
    }

    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
