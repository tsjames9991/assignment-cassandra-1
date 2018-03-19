package org.knoldus

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

package object model {
  val SALARY = 45000
  val ID = 2
  val log = Logger.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()
  val keyspace: String = config.getString("cassandra.keyspace")
  val hostName: String = config.getString("cassandra.contact.points")

  def getCassandraSession: Session = {

    val cluster = new Cluster.Builder().withClusterName("Test Cluster")
      .addContactPoint(hostName).build()
    cluster.connect()
  }
}
