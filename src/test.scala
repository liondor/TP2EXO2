import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}
import org.apache.spark.rdd.RDD


trait node {
val id: String;
var  hp: Int;
  val attack: Int;
val armure: Int;
val vitesse : Int;
  var position :Int;
  def launchAttack () : Int =
  {
    attack
  }
  override def toString: String = s"id : $id hp : $hp attack : $attack armor : $armure  vitesse : $vitesse position : $position\n"
}
class VertxProperty()
case class Solar(val id: String= "Solar", var  hp : Int=363,  val attack : Int =0, val armure: Int=44, val vitesse : Int=50, var position: Int= scala.util.Random.nextInt(500)) extends node
{
  override def launchAttack() : Int =
  {
    val r = scala.util.Random
    var res = attack+ r.nextInt(5)+r.nextInt(5)+r.nextInt(5)+3;
    res
  }

}
case class Warlord( val id: String="Warlord", var hp: Int =141,val attack : Int =0,armure: Int=27,vitesse: Int = 30, var position: Int= scala.util.Random.nextInt(500)) extends node
{
  override def launchAttack() : Int =
  {
    val r = scala.util.Random
    var res = attack+ r.nextInt(5)+r.nextInt(5)+r.nextInt(5)+3;
    res
  }

}
object test extends App {
  def applyDamage(vId :VertexId, perso : node ,dommage : Int) : node =
  {
    var res = Solar();
    res.hp= perso.hp-dommage
    res

  }

  import org.apache.spark.{SparkConf}
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}

    val conf = new SparkConf()
      .setAppName("Combat 1")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   /* val solar : Node = new Node(id="Solar",hp=363,armure=44,vitesse =50)
    val warlord : Node = new Node(id="Warlord",hp=141,armure=27,vitesse = 30,)
    val worgs : Node = new Node(id="Worg",hp=13,armure=18,vitesse = 20)
    val orc : Node = new Node(id="Orc",hp=142,armure=17,vitesse = 40)*/
   var myVertices : RDD[(VertexId,node)]= sc.parallelize(Array((1L,new Solar()),(2L,new Warlord()),(3L,new Warlord())))
  var myEdges = sc.parallelize(Array(Edge(1L,2L,"1"),Edge(1L,3L,"2")))
  var myGraph = Graph(myVertices, myEdges)
  print("----- Graphe de base ----")
  myGraph.triplets.foreach(print(_) )
  print("\n")
  val messages = myGraph.aggregateMessages[Int](
//   sendAttack,
  //  mergeAttack,
    triplet => {
      //if(scala.math.abs(triplet.srcAttr.position-triplet.dstAttr.position)<10)
        //{
      /*    triplet.sendToDst(triplet.srcAttr.launchAttack())
          triplet.sendToSrc(triplet.dstAttr.launchAttack())
        */
          triplet.sendToSrc(triplet.dstAttr.position)

        //}

    },
    {
      (a,b) => {
  //      print(a+"\n")
    //    print(b+"\n")
        if(a<b)
          {
            a
          }
        else{
          b
        }

      }
    }
  )

  //messages.foreach(print(_))
  val damage = myGraph.aggregateMessages[node](
    triplet=>{
      if(triplet.dstAttr.position==messages.collect().last._2) {
        triplet.sendToSrc(triplet.dstAttr)
        triplet.sendToDst(triplet.srcAttr)
      }
    },
    (a,b) => {
      a
    }
  )


  myGraph = myGraph.joinVertices(damage)(
    (vid, personnage, msg) => applyDamage(vid, personnage, msg.launchAttack()))
  print("----- Nouveau graphe  ----")
  myGraph.triplets.foreach(print(_))

}
/*def sendAttack(ctx: EdgeContext[Node, Node, Long]): Unit = {
    ctx.sendToDst(ctx.srcAttr.armure)
    ctx.sendToSrc(ctx.dstAttr.armure)

}
def pick(val attack1 :Int,val attack2 :Int)
 {
   attack1
 }
*/
