package stage.Pfe
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}

object KafkaProducter {
  def main(args: Array[String]): Unit = {
     
    // 1.0 Disable the logger 
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
    // 2.0 Test arguments 
            if(args.length < 4 )
            {
              //System.err.println(" Usage :  <Hostname> <Port>");
              System.err.println("Usage: KafkaProducter <TopicName> <FileName> <LineNumber> <SleepDuration>")
              System.exit(1)
            }    
       val  props = new Properties()
       props.put("bootstrap.servers", "localhost:9092")
        
       props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
       props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      
       val producer = new KafkaProducer[String, String](props)
         
       val TOPIC=args(0)
       val filename = args(1)
       var  nbline= args(2).toInt;
       val  durationWindow = args(3).toInt;
       
       try {
               for (line <- Source.fromFile(filename).getLines()) 
               {
                   //println(line)
                   val record = new ProducerRecord(TOPIC, "key", s"$line")
                   producer.send(record)
                   // test 
                   nbline-=1;
                   if(nbline==0)
                   {
                      Thread.sleep(durationWindow)
                      nbline= 3
                   }
               }
               
               
           } catch {
              case ex: FileNotFoundException => println("Couldn't find that file.")
              case ex: IOException => println("Had an IOException trying to read that file")
            }
    
       println(" End OF File with Success ")
       producer.close()


  } 
}