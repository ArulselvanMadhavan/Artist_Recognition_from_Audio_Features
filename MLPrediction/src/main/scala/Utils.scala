import java.io.{BufferedWriter, File, FileWriter}

object Utils {

  def writeToFile(path: String,content: String): Unit = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file, true))
    bw.write(content)
    bw.close()
  }

}