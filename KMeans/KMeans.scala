package KMeans

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class KMeans extends Serializable {
  var cluster = 2 //分类数量
  private val shold = 0.000000001  //阈值
  private var DataListPerInteval: ArrayBuffer[(Double, Double)] = ArrayBuffer[(Double,Double)]()//将RDD转化为Array
  private var centerArray: ArrayBuffer[(Double, Double)] = ArrayBuffer[(Double,Double)]()  //转化完之后找样本的随机中心点

  //初始化随机中心点
  def initialCenters(rdd:RDD[(Double,Double)]) :Unit={
    if(rdd.count() >= 4) {
      val data = rdd.collect()
      for (i <- 0 until data.length - 1) {
        println(data(i))
        DataListPerInteval += data(i)
      }
      var centerPoint = new ArrayBuffer[(Double, Double)]()
      val indexMap = mutable.HashMap[Int, Int]()
      var index: Int = 0
      while (indexMap.size != cluster) {
        index = Random.nextInt(DataListPerInteval.length)
        indexMap += (index -> index)
      }
      indexMap.keys.map(x =>
        centerPoint += DataListPerInteval(x)
      ) //去重
      centerArray = centerPoint
      start()
    }else{
      println("内部数据不够")
    }
  }

  def start() = {     //方法主入口
    var newCenters = ArrayBuffer[(Double,Double)]()
    var newCost = 0.0
    var currentCoust:Double = 0
    var flag = true
    while(flag){
      currentCoust = computeCost(DataListPerInteval,centerArray)
      val cluster = DataListPerInteval.groupBy(point => nearlistCenter(centerArray,point))
      newCenters =
        centerArray.map(oldCenter => {
          cluster.get(oldCenter) match {  //聚类中心里的所有点的合集
            case Some(pointsInThisCluster) =>
              //均值做聚类中心}
              pointDivid(pointsInThisCluster.reduceLeft((point1,point2) => pointAdd(point1,point2)),pointsInThisCluster.length)
            case None => oldCenter
          }
        })
      for(i <- 0 until centerArray.length-1){     //新的中心点集合
        centerArray(i) = newCenters(i)
      }
      newCost = computeCost(DataListPerInteval,centerArray)     //新的代价函数
      if(math.sqrt(pointDistance((currentCoust,0),(newCost,0)))<shold) {flag = false}
    }
    println("当前批次的数据在  KMEANS 算法中最优中心点与对应的是:" + newCenters.toBuffer)
    DrawResult(newCenters)
  }

  private def nearlistCenter(center: ArrayBuffer[(Double,Double)], v:(Double,Double)) ={     //为中心点分数据的Reduce高阶函数
    center.reduceLeft((c1,c2) => {
      if(pointDistance(c1,v) < pointDistance(c2,v)) c1 else c2
    })
  }

  //计算每个样本点到聚类中心的距离之和
  def computeCost(points: ArrayBuffer[(Double,Double)],center: ArrayBuffer[(Double,Double)]):Double={
    val cluster = points.groupBy( v => nearlistCenter(center,v))
    var costSum = 0.0
    for(i <- 0 until center.length-1){
      cluster.get(center(i)) match {
        case Some(subSets) => {
          for (j <- 0 to subSets.length-1){
            costSum += (pointDistance(center(i),subSets(j)) * pointDistance(center(i),subSets(j)))
          }
        }
        case None => costSum = costSum
      }
    }
    costSum
  }

  private def pointDistance(point1: (Double,Double),point2: (Double,Double)): Double ={     //点距离
    var distance = (point1._1-point2._1)*(point1._1-point2._1)+(point1._2-point2._2)*(point1._2-point2._2)
    distance = sqrt(distance)
    distance
  }

  private def pointAdd(point1: (Double,Double),point2: (Double,Double)): (Double,Double) ={     //点和
    (point1._1 + point2._1,point2._2 + point2._2)
  }

  private def pointDivid(point: (Double,Double),num: Int): (Double,Double)={    //均值
    (point._1/num,point._2/num)
  }


  def DrawResult(resultCenter: ArrayBuffer[(Double,Double)]) = {
    println("画图结果")
  }
}
