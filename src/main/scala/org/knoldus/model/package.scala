package org.knoldus

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}

package object model {

  val config: Config = ConfigFactory.load()
  val keyspace: String = config.getString("cassandra.keyspace")
  val hostName: String = config.getString("cassandra.contact.points")

  def getCassandraSession: Session = {

    val cluster = new Cluster.Builder().withClusterName("Test Cluster")
      .addContactPoint(hostName).build()
    cluster.connect()
  }
}
