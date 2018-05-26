package stage.Pfe
import java.io._

@SerialVersionUID(100L) 
class Items( label: String ,var  value : Array[Boolean]) extends Serializable {

  // getters
  def getLabel()= label 
  def getValue = value 
  
  def addForValue( elem : Boolean ){
    value.:+(elem)
  }
  /**
   * toString  method 
   */
  override def toString: String =  "(" + label + ", "+value.toList+" )"
  /**
   * method  Add 
   */
   def add(item : Items):Items={
    // concat  les deux label  
      val  label= item.getLabel()+" " + this.label;
    // faire le OR LOgique entre les deux Array 
     var array:Array[Boolean] =  Array()
         for(i<- 0 to value.length-1)
         {
           var element = this.value(i) || item.getValue(i)  
           array:+=element
         }
      var aux = new Items(label, array)
     return  aux
  }
  /**
   *   method  verifier la comb. est  possible ou  pas  
   */
  def  verifier(item : Items ) : Boolean ={
     
    var  ok1 = false  
    var  ok2 = false  
    for ( i <- 0 to  value.length -1 )
    {
          if ( value(i) && ! item.getValue(i))  ok1 = true ;   
          if ( !value(i) && item.getValue(i))  ok2 = true ; 
          if ( ok1 && !value(i) && item.getValue(i)) return  true ;
          if ( ok2 && value(i) && ! item.getValue(i))  return true ;
    }

    return false  
  } 

  /**
   *  method equals --> boolean
   *  
   */
    override def hashCode() : Int = {
     return  label.hashCode()
    }
   def canEqual( a: Any ) = a.isInstanceOf[Items]
   override def equals (that : Any ) : Boolean  ={
    that match {
      case that :Items  => that.canEqual(this) && this.hashCode() == that.hashCode()
    }
  
  }
}