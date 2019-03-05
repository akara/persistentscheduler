package com.paypal.squbs.scheduler

import java.net.InetAddress

import com.ebay.kernel.context.AppBuildConfig

trait NodeIdProvider {
  def nodeId: String
  def clusterName: String
}

class CronusNodeIdProvider extends NodeIdProvider {

  val defaultClusterName = "DefaultTestCluster"

  def nodeId: String = InetAddress.getLocalHost.getHostName

  def clusterName: String =
    Option(System.getenv("CLUSTER_NAME")) orElse
      Option(AppBuildConfig.getInstance.getCalPoolName) getOrElse
      defaultClusterName
}
