import scala.math._

 import scala.util.Try

object trialanderror
{
  println("Welcome to the Scala worksheet")

def arrayMaximalAdjacentDifference(inputArray: Array[Int]): Int = {


var res =   inputArray.foldLeft(0) { (acc, x) =>
          if (x < acc) min(x, acc)
          else if (x > acc) min(acc, x)
          else acc
          }
   
4

}
def isIPv4Address(xy: String): Boolean = {
var checkformatexception = xy.split("\\.").toList.map(x=> Try(x.toInt).isFailure)
var result = true
println(checkformatexception)
if(!checkformatexception.contains(true) )
{
var xyz1 = xy.split("\\.").toList.map(x => if(x.toInt<=255)true else false)
println(xyz1)
if(xyz1.contains(false) || xyz1.size!=4)result = false else result = true
}
else
{
result = false
}
result
}

def avoidObstacles(inputArray: Array[Int]): Int = {
var counter = 0
var sorteddata = inputArray.sorted
println(sorteddata.toList)
for(i <- 0 until inputArray.length-1)
{
if(!(i == sorteddata(i)))counter=counter+1 else sorteddata(i+1)
}
counter
}






var inputstring = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaabc"
var inputArray = Array(-1, 4, 10, 3, -2)
arrayMaximalAdjacentDifference(inputArray)
var a = "1.1.1.1.1"
println(isIPv4Address(a))



var  q = Array(5, 3, 6, 7, 9)
println(avoidObstacles(q))
}