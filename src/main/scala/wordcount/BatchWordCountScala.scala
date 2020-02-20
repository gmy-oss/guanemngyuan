package wordcount

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScala {

  def main(args: Array[String]): Unit = {
   val inputpath =  "D:\\data\\file"
    val outputpath = "D:\\data\\result"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputpath)

    import org.apache.flink.api.scala._

    val count = text.flatMap(_.toLowerCase.split("\\w+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1).setParallelism(1)


    count.writeAsCsv(outputpath,"\n"," ")
    env.execute("batch word count")
  }
}
