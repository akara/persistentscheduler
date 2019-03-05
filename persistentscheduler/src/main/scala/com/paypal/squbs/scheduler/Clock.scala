package com.paypal.squbs.scheduler

trait Clock {
  def currentTimeMillis: Long
}

class WallClock extends Clock {
  override def currentTimeMillis: Long = System.currentTimeMillis
}
