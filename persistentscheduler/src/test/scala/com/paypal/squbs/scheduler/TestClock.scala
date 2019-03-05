package com.paypal.squbs.scheduler

class TestClock extends Clock {
  /**
    * @return A static time for test predictability.
    */
  override def currentTimeMillis: Long = 10001
}
