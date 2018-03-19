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

  private def createTable(session: Session): Unit = {

    session.execute("create table if not exists employee (id int, name text, city text,salary varint, phone varint, primary key(id,salary))")
    session.execute("create table if not exists employee1(id int, name text, city text,salary varint, phone varint, primary key(city))")
  }

  private def getAllRecord(session: Session): Unit = {
    val record = session.execute("select * from employee").asScala.toList
    record.foreach(println(_))
  }

  private def getRecordById(session: Session, employee_id: Int): Unit = {

    val record = session.execute(s"select * from employee where id=${employee_id}").asScala.toList
    record.foreach(println(_))
    println()
  }

  private def updateRecord(session: Session, id: Int, salary: Int): Unit = {
    session.execute(s"update employee set city='chandigarh' where id=${id} AND salary=${salary}")
    getAllRecord(session)
  }

  private def getRecordBySalary(session: Session, salary: Int, id: Int): Unit = {
    val record = session.execute(s"select * from employee where id =${id} AND salary > ${salary} ")
    println("fetch record salary where greater than 30000")
    record.forEach(println(_))
    println()
  }

  private def getRecordByCity(session: Session, city: String): Unit = {

    session.execute("create index if not exists cityIndex on employee(city)")
    val record = session.execute(s"select * from employee where city ='${city}'").asScala.toList
    record.foreach(println(_))
    println()
  }

  private def deleteRecordByCity(session: Session, city: String): Unit = {

    session.execute(s"delete from employee1 where city = '${city}'")
    val record = session.execute("select * from employee1").asScala.toList
    record.foreach(println(_))
  }

  private def insertRecord(session: Session): Unit = {

    session.execute("insert into employee(id,name,city,salary,phone) values(2,'Sudeep James Tirkey','Ghaziabad',12000,9810622717)")
    session.execute("insert into employee(id,name,city,salary,phone) values(1,'Vinay Kuamr','Gurgaon',12000,9910645994)")
    session.execute("insert into employee(id,name,city,salary,phone) values(1,'Abhishek Trivedi','Kanpur',14000,886089556)")
    session.execute("insert into employee(id,name,city,salary,phone) values(4,'Shahab Khan','Delhi',10000,9811660867)")

    session.execute("insert into employee1(id,name,city,salary,phone) values(2,'Sudeep James Tirkey','Ghaziabad',12000,9810622717)")
    session.execute("insert into employee1(id,name,city,salary,phone) values(1,'Vinay Kuamr','Gurgaon',12000,9910645994)")
    session.execute("insert into employee1(id,name,city,salary,phone) values(3,'Abhishek Trivedi','Kanpur',14000,886089556)")
    session.execute("insert into employee1(id,name,city,salary,phone) values(4,'Shahab Khan','Delhi',10000,9811660867)")
  }
}
