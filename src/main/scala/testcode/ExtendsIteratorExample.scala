package testcode

import scala.collection.mutable.ListBuffer

/**
 * Created by nkatz at 28/1/20
 */

object ExtendsIteratorExample extends App {

  /**
   * General blueprint for extending an iterator and performing data pre-processing within its next() method.
   *
   * */

  /**
   * A very simple exercise, we're reading a large source of numbers and we're grouping them into mini-batches.
   * There are two ways to do it. The first reads all the data from the start, the second does it lazily.
   *
   * */

  type MiniBatch = Seq[Int] // just a helper type, you won't need it to modify your code.


  /**
   * This is close to what you have been doing.
   * You are processing all the data with a while loop inside the function, so its natural
   * that everything happens when you call the function.
   * */
  def wrong(inputSource: Iterator[Int], batchSize: Int) = {

    var buffer = new ListBuffer[Int]()
    var batchCounterSize = 0
    val batches = new ListBuffer[MiniBatch]()

    while(inputSource.hasNext) {
      while (batchCounterSize < batchSize) {
        buffer.append(inputSource.next())
        batchCounterSize += 1
      }
      batches.append(buffer.toVector)
      batchCounterSize = 0
      buffer = new ListBuffer[Int]()
    }
    batches.toIterator
  }

  /**
   *
   * This is a way to do it lazily. You define a new iterator class that does all the work into its next() method.
   * So every time you call next() you get a new mini-batch, which you can do whatever you like.
   *
   * Here inputSource is just an iterator of numbers.
   * In your function it should be the iterator you get by reading the input file
   * (val data = Source.fromFile(dataPath).getLines), so its an Iterator[String].
   *
   * This class extends Iterator[MiniBatch], which means that its next()
   * method returns a mini-batch of the input number sequence. In your code it should return an Iterator[Example].
   *
   * */
  class ExampleIterator(val inputSource: Iterator[Int], val batchSize: Int) extends Iterator[MiniBatch] {

    def hasNext = inputSource.hasNext

    // This is where everything happens. You should modify this method to read lines
    // from the input, form mini-batches and convert each mini-batch into an Example instance.
    // So all the logic goes here. You get one mini-batch/example upon each next() call.
    def next = {
      val buffer = new ListBuffer[Int]()
      var batchCounterSize = 0
      while (batchCounterSize < batchSize) {
        buffer.append(inputSource.next())
        batchCounterSize += 1
      }
      buffer
    }
  }

  /**
   * This is the actual function that returns the iterator of examples.
   * When you call this function nothing happens. Everything happens when you
   * process the iterator that the function returns as a result.
   *
   *
   * */
  def right(inputSource: Iterator[Int], batchSize: Int) = new ExampleIterator(inputSource, batchSize)

  // Utility function to time the execution of stuff.
  def time[R](codeBlock: => R): (R, Double) = {
    val t0 = System.nanoTime()
    val result = codeBlock
    val t1 = System.nanoTime()
    val totalTime = (t1 - t0) / 1000000000.0
    (result, totalTime)
  }


  /**
   * Tests
   * */

  val (miniBatchesWrong, time1) = time { wrong(1 to 10000000 toIterator, 10) }

  println(s"Time for 'wrong' is: " + time1)

  val (miniBatchesRight, time2) = time { right(1 to 10000000 toIterator, 10) }

  println(s"Time for 'right' is: " + time2)

  println("The difference is that 'wrong' generates a val (miniBatchesWrong) after reading and performing operations on all the data. 'right' does it lazily.")

  //miniBatchesRight foreach println

}
