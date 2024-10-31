import org.apache.spark.SparkContext

object BASICRDDPROBLEMS {
  def main(arr:Array[String]): Unit = {
    //step 1 creating the Sparkcontest//
    val sc=new SparkContext("local[4]","chaithu")
    /*//reading the data from textfile
    val input=sc.textFile("C:/Users/poola/Desktop/chaithanya.txt")
    val rdd1=input.flatMap(x=>x.split(" "))
    val rdd2=rdd1.map(x=>(x,1))
    val rdd3=rdd2.reduceByKey((x,y)=>(x+y))
    //if we want to printouput in the descendong order then use sortby
    val rdd4=rdd3.sortBy(x=>x._2,true)
    rdd4.collect().foreach(println)*/
    /*val arr=Array(1,2,3,4,5)
    val rdd1=sc.parallelize(arr)
    val rdd2=rdd1.filter(x=>x%2==0)
    val rdd3=rdd2.map(x=>(x*100))
    rdd3.collect().foreach(println)*/
    //ALL ABOUT JOIN AND OUTPUTS
    /*val arr=Array((1,"chaithu"),(2,"hydra"),(3,"lucifer"),(4,"hello"))
    val arr2=Array((1,"gdgdgd"),(2,"hdhbdgd"),(0,"zxaqw"),(7,"asdf"))
    val rdd1=sc.parallelize(arr)
    val rdd2=sc.parallelize(arr2)
    //Join Means we Always get the Matching records From BOth The Tables
    //val rdd3=rdd1.join(rdd2)
    // Elements + Rightside Some means we Used the Left join
    // Elements + Leftside Some means we Used the Right join
    // Elements With 2 somes if we Seeing In The Output Means We are Using the Full join Here
    // Cartiesin join Means each left element with each Right Element
    val rdd3=rdd1.cartesian(rdd2)
    rdd3.collect().foreach(println)*/
    /*val arr=Array(1,2,3,4,5)
    val rdd1=sc.parallelize(arr)
    val rdd2=rdd1.mean()
    print(rdd2)*/
    //FINDING MEAN WITHOUT MEAN FUNCTION
    /*val arr=Array(1,2,3,4,5,6,7,81)
    val rdd2=sc.parallelize(arr)
    val rdd3=rdd2.sum()
    val rdd4=rdd2.count()
    val rdd5=rdd3/rdd4
    print(rdd5)*/
    //setoperators
    /*val arr=Array(1,2,3,4,5)
    val arr1=Array(1,2,3,4,5,6)
    val rdd1=sc.parallelize(arr)
    val rdd2=sc.parallelize(arr1)
    //Subtranct means ONLY unique values fom 2 Arrays
    //val rdd3=rdd2.subtract(rdd1)
    //INtersection Means Only common values from 2 tables
    //val rdd3=rdd2.intersection(rdd1)
    //Union Means i will print all the records in Both the Tables
    val rdd3=rdd2.union(rdd1)
    rdd3.collect().foreach(println)*/
    /*val arr=Array(1,2,3,4,5)
    val rdd1=sc.parallelize(arr)
    //Filtering and Multiple the O/P with 100
    val rdd2=rdd1.filter(x=>x%2==0)
    val rdd3=rdd2.map(x=>x*100)
    rdd3.collect().foreach(println)*/
    //Counting The no of words
    /*val arr=Array("chaithu","hydra","chaithu","lucifer","chaithu","chaithu")
    val rdd1=sc.parallelize(arr)
    val rdd2=rdd1.flatMap(x=>x.split(","))
    val rdd3=rdd2.map(x=>(x,1))
    val rdd4=rdd3.reduceByKey((x,y)=>(x+y))
    //printing all the Values
   // rdd4.collect().foreach(println)
    //print only TOP Two values then
    //Using Sortby we can sort BOTH keys and values only differnce is handle the Tuples carefully
    val rdd5=rdd4.sortBy(x=>x._1)
    rdd5.take(2).foreach(println)*/

  }

}
