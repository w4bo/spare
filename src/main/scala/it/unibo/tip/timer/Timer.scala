package it.unibo.tip.timer

/**
 * Simple timer trait.
 */
trait Timer {

  /**
   * Get time elapsed since start in millis.
   */
  def getTimeInMillis():Long

  /**
   * Get time elapsed since start in seconds.
   */
  def getTimeInSeconds():Long

}

object  Timer {

  def apply(): Timer = new TimerImpl()

  private class TimerImpl() extends Timer {
    val startTime = System.currentTimeMillis()

    override def getTimeInMillis(): Long = System.currentTimeMillis() - startTime

    override def getTimeInSeconds(): Long = this.getTimeInMillis() / 1000
  }
}
