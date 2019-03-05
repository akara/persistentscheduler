package com.paypal.squbs.scheduler

class TestNodeIdProvider extends NodeIdProvider {
  override val nodeId: String = "thisNode"

  override val clusterName: String = "unitTest"
}
