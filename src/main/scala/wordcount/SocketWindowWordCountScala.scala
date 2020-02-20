package wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 滑动窗口计算
 *
 * 每隔一秒统计最近2秒内的数据 打印到控制台
 *
 **/
object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {

//    获取socket端口号
    val port: Int = try {
    ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception => {
        System.err.println("No post set . use default port 9000 --- scala")
      }
        9000
    }


    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val text = env.socketTextStream("hadoop102",port,'\n')

//    解析数据  分组  窗口计算  并且聚合求sum

    import org.apache.flink.api.scala._

    val windowCount = text.flatMap(line => line.split("\\s"))//打平，把每一行单词切开
      .map(w => WordWithCount(w,1))//把单词转成word ,1这样的形式
      .keyBy("word").timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")

windowCount.print().setParallelism(1)

    env.execute()

  }

  case class WordWithCount(word:String,count:Long)
}
