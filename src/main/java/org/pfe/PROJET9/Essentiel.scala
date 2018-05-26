package org.pfe.PROJET9;
/**
 *  Network Essentiel Items  v 1.0 
 * 	@Author : Dagbouj Hatem + El Ghoul Saber
 * 	@date : 15-04-2017 
 * 
 *  compile with  :  nc -lc 9999
 * 	add to run configuration : localhost 9999
 * 	add JVM arguments : -Xmx1024m     --> add memory  to  JVM
 */

import org.apache.log4j.Logger
import org.apache.log4j.Level
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.collection.mutable.ListBuffer
import org.apache.spark.util._


/**
 * Use this Accumulator to Count the number  of Transaction .
 */
object TransactionsNumber {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("NombreDeTransactions")
        }
      }
    }
    instance
  }
}
/**
 * Use this Accumulator to Items Distinct  .
 */
object ListOfItems {

  @volatile private var instance: CollectionAccumulator[Items] = null

  def getInstance(sc: SparkContext): CollectionAccumulator[Items] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.collectionAccumulator[Items]("listeofitems")
        }
      }
    }
    instance
  }
}
/**
 * Use this Accumulator to Items Essentiel  .
 */
object ListOfItemsEssentiel {

  @volatile private var instance: CollectionAccumulator[Items] = null

  def getInstance(sc: SparkContext): CollectionAccumulator[Items] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.collectionAccumulator[Items]("listeofitemsessentiel")
        }
      }
    }
    instance
  }
}
object Essentiel {
  
  def verifierExistance(arr1 : Array[String] , arr2 : Array[String]): Boolean = {
    
    for ( i <- 0 to  arr1.length-1)
    {
      if(arr2.contains(arr1(i))) return  true ; 
    }
    return false; 
  }
    
  def main(args: Array[String]): Unit = {
     
        
    // 1.0  arguments verification 
    if(args.length < 4 )
    {
      //System.err.println(" Usage :  <Hostname> <Port>");
      System.err.println("Usage: Essentiel <host> <port> ")
      System.exit(1)
    }
    // 2.0 create the Spark  Configuration  
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[3]")
    // 3.0 Create the Spark Streaming context with a 60 second batch size 
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    // 4.0 Create a socket stream on target ip:port 
     val kafkaParam=Map("metadata.broker.list"->"localhost:9092")
    val topics=List("test").toSet
    val reciver =KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParam, topics).map(_._2)
         // ssc.checkpoint("checkpoint") // checkpoint  folder  
    // 6.0 Algorithme 
            
            // liste des transaction 
            val dstream = reciver.flatMap(_.split("\n")).persist 
            // appliquer  a toute les transaction       
            dstream.foreachRDD { rdd  =>  
 
                    var trasactions = rdd.collect();   // liste des transaction  
                    val number = TransactionsNumber.getInstance(rdd.sparkContext) // Accumulator :  nombre de trasaction  
                    val listItems=  ListOfItems.getInstance(rdd.sparkContext)     // Acumulator  :  liste des items Distinct 
                    val listItemsEssentiel=  ListOfItemsEssentiel.getInstance(rdd.sparkContext)  // Accumulator : liste des Items Essentiel 
                    // lires les item
                    trasactions.foreach { t =>  
                      
                       number.add(1)  // incrimonter le nombre de transactions 
                       var  itemsTransaction= t.trim().split(" ") 
                                      /**
                                       *  ajouter (True / False ) à tous les items selon  
                                       */
                                     var coll = listItems.copy()
                                      listItems.reset()
                                      for(index <- 0 to coll.value.size()-1)
                                      {
                                        var  arr= coll.value.get(index).value.:+(itemsTransaction.contains(coll.value.get(index).getLabel()))
                                        listItems.add( new Items(coll.value.get(index).getLabel() , arr))
                                        
                                      }  
                                      var ess : Set [Items]= Set()
                       itemsTransaction.foreach
                       { item =>
                           var itemsObject = new Items(item , Array())
                           
                           if (!listItems.value.contains(itemsObject)) // l'item  appaitre le premier fois 
                           {   if( number.value == 1 ) // si  le 1ere transaction  
                                   listItems.add(new Items(itemsObject.getLabel(),Array(true)))
                               else
                               {   // si c'est  pas le  1ere Transaction
                                 
                                 itemsObject.value =Array.ofDim[Boolean](number.value.toInt-1).:+(true) 
                                  listItems.add(itemsObject) 
                                  
                                 ess=ess.+(new Items( item ,Array.ofDim[Boolean](number.value.toInt-1)))
                                  
                               }
                              
                             
                           } 
                       } // fin de chaque transaction  
                       
                       /**
                        *  calcul  des  essentiel  
                        */ 
                      
                       if(number.value==1)
                       {
                                      var coll = listItems.copy() 
                                      for(index <- 0 to coll.value.size()-1)
                                      { 
                                        listItemsEssentiel.add(coll.value.get(index)) 
                                      }  
                             //Affichage des essentiels
                            println("Tr n ° "+ number.value +"List Essentiel :" + listItemsEssentiel.value )
                       }
                       else{   /** faire le produit */
                                var coll = listItemsEssentiel.copy() 
                                          
                               
                                      for(index <- 0 to coll.value.size()-1)
                                      { 
                                        var  ob1= new Items(coll.value.get(index).getLabel(), 
                                             coll.value.get(index).value.:+(verifierExistance(coll.value.get(index).getLabel().split(" ") ,itemsTransaction )))
                                              var it = listItems.value.iterator()
                                              while(it.hasNext()) 
                                               {   var  ob2 = it.next()
                                                     if(ob1.verifier(ob2)) 
                                                        {   
                                                             var ob3 = ob1.add(ob2)
                                                             if(!listItemsEssentiel.value.contains(ob3))
                                                               {     
                                                                     listItemsEssentiel.add(ob3)
                                                               } 
                                                        } 
                                               }
                                        
                                      }  
                                ess.map { x =>   listItemsEssentiel.add(x)}
                                ess= Set()
                            //Affichage des essentiels
                            println("Tr n ° "+ number.value +" List Essentiel:" + listItemsEssentiel.value )
                              
                       }
                       println("Tr n ° "+ number.value +" List Items    :"+ listItems.value)
                       
                    }

                     
               
            } 

     ssc.start()             // Start the computation
     ssc.awaitTermination()  // Wait for the computation to terminate
  }
}