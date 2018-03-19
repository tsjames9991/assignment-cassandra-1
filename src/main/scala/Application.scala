import org.knoldus.model._

object Application extends App {
  val result = new Operations
  result.insertRecord()
  log.info(s"Data fetched by Id\n")
  result.getRecordById(ID)
  log.info(s"Data after update\n")
  result.updateRecord(ID,SALARY)
  log.info(s"Data fetched by salary and id\n")
  result.getRecordBySalary(ID,SALARY)
  log.info(s"Data fetched by city\n")
  result.getRecordByCity("Ghaziabad")
  log.info(s"Data post deletion\n")
  result.deleteRecordByCity("Kanpur")
}
