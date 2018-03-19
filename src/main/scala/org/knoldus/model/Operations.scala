package org.knoldus.model

import com.datastax.driver.core.{ConsistencyLevel, Session}
import scala.collection.JavaConverters._

class Operations {
  val cassandraSession: Session = getCassandraSession
  cassandraSession.getCluster.getConfiguration.getQueryOptions
    .setConsistencyLevel(ConsistencyLevel.QUORUM)

  def createKeyspace: String = {
    cassandraSession.execute(s"CREATE KEYSPACE IF NOT EXISTS ${keyspace} " +
      s"WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }")
    cassandraSession.execute(s"USE ${keyspace} ")
    s"Keyspace has been created!"
  }

  private def createTable(): Unit = {

    cassandraSession.execute("create table if not exists employee (id int, name text, city text,salary varint, phone varint, primary key(id,salary))")
    cassandraSession.execute("create table if not exists employee1(id int, name text, city text,salary varint, phone varint, primary key(city))")
  }

  def getAllRecord(): Unit = {
    val record = cassandraSession.execute("select * from employee").asScala.toList
    record.foreach(data => log.info(data))
  }

  def getRecordById(employee_id: Int): Unit = {

    val record = cassandraSession.execute(s"select * from employee where id=${employee_id}").asScala.toList
    record.foreach(data => log.info(data))

  }

  def updateRecord(id: Int, salary: Int): Unit = {
    cassandraSession.execute(s"update employee set city='chandigarh' where id=${id} AND salary=${salary}")
  }

  def getRecordBySalary(salary: Int, id: Int): Unit = {
    val record = cassandraSession.execute(s"select * from employee where id =${id} AND salary > ${salary} ")
    log.info("fetch record salary where greater than 30000")
    record.forEach(data => log.info(data))
  }

  def getRecordByCity(city: String): Unit = {

    cassandraSession.execute("create index if not exists cityIndex on employee(city)")
    val record = cassandraSession.execute(s"select * from employee where city ='${city}'").asScala.toList
    record.foreach(data => log.info(data))
  }

  def deleteRecordByCity(city: String): Unit = {

    cassandraSession.execute(s"delete from employee1 where city = '${city}'")
    val record = cassandraSession.execute("select * from employee1").asScala.toList
    record.foreach(data => log.info(data))
  }

  def insertRecord(): Unit = {

    cassandraSession.execute("insert into employee(id,name,city,salary,phone) values(2,'Sudeep James Tirkey','Ghaziabad',12000,9810622717)")
    cassandraSession.execute("insert into employee(id,name,city,salary,phone) values(1,'Vinay Kuamr','Gurgaon',12000,9910645994)")
    cassandraSession.execute("insert into employee(id,name,city,salary,phone) values(1,'Abhishek Trivedi','Kanpur',14000,886089556)")
    cassandraSession.execute("insert into employee(id,name,city,salary,phone) values(4,'Shahab Khan','Delhi',10000,9811660867)")

    cassandraSession.execute("insert into employee1(id,name,city,salary,phone) values(2,'Sudeep James Tirkey','Ghaziabad',12000,9810622717)")
    cassandraSession.execute("insert into employee1(id,name,city,salary,phone) values(1,'Vinay Kuamr','Gurgaon',12000,9910645994)")
    cassandraSession.execute("insert into employee1(id,name,city,salary,phone) values(3,'Abhishek Trivedi','Kanpur',14000,886089556)")
    cassandraSession.execute("insert into employee1(id,name,city,salary,phone) values(4,'Shahab Khan','Delhi',10000,9811660867)")
  }
}
