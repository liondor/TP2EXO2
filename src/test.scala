import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}
import org.apache.spark.rdd.RDD


trait node {
val id: String;
var  hp: Int;
  val attack: Int;
val armure: Int;
val vitesse : Int;
  var position :Int;
  val porteMax : Int;
  def launchAttack () : Int =
  {
    attack
  }

  override def toString: String = s"id : $id hp : $hp attack : $attack armor : $armure  vitesse : $vitesse position : $position porté Maximale : $porteMax\n"
}

case class Solar(id: String= "Solar", var  hp : Int=363,  attack : Int =18,  armure: Int=44, vitesse : Int=50, var position: Int= scala.util.Random.nextInt(500), porteMax : Int = 110) extends node
{
  override def launchAttack() : Int =
  {
    val r = scala.util.Random
    var res = attack+ r.nextInt(5)+r.nextInt(5)+r.nextInt(5)+3;
    res
  }

}
case class Warlord( id: String="Warlord", var hp: Int =141,attack : Int =10,armure: Int=27,vitesse: Int = 30, var position: Int= scala.util.Random.nextInt(500), porteMax : Int = 10) extends node
{
  override def launchAttack() : Int =
  {
    val r = scala.util.Random
    var res = attack+ r.nextInt(7)+1;
    res
  }

}
object test extends App {
  def applyDamage(vId :VertexId, perso : node ,dommage : Int) : node =
  {

    perso.hp= perso.hp-dommage
    perso

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
  //myGraph.triplets.foreach(print(_) )
  myGraph.vertices.foreach(print(_))
  print("\n")
  /* Premier aggregate pour trouver l'ennemi le plus proche*/
  val messages = myGraph.aggregateMessages[Int](
//   sendAttack,
  //  mergeAttack,
    triplet => {

          triplet.sendToSrc(triplet.dstAttr.position)
    },
    {
      (a,b) => {
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
  /* Deuxième  pour l'attaquer si il est assez proche 'pas encore implémenter la porté)*/
  val damage = myGraph.aggregateMessages[Int](
    triplet=>{
      if(triplet.dstAttr.position==messages.collect().last._2) {
        triplet.sendToDst(triplet.srcAttr.launchAttack())
      }
    },
    (a,b) => {
      a+b
    }
  )
  /* On regarde ce que les ennemis vont faire !*/
  val damageEnemy = myGraph.aggregateMessages[Int](
    triplet=>{
      //if(scala.math.abs(triplet.dstAttr.position-triplet.srcAttr.position)<=triplet.dstAttr.porteMax+500) {
        triplet.sendToSrc(triplet.dstAttr.launchAttack())

     // }
    },
    (a,b) => {
      a+b
    }
  )
  print("----- Test ----\n")
  //myGraph.triplets.foreach(print(_) )
  damageEnemy.foreach(print(_))
  print("\n")
  myGraph = myGraph.joinVertices(damage)(
    (vid, personnage, msg) => applyDamage(vid, personnage, msg))
  myGraph.vertices.foreach(print(_))
  myGraph = myGraph.joinVertices(damageEnemy)(
    (vid, personnage, msg) => applyDamage(vid, personnage, msg))
  print("----- Nouveau graphe  ----\n")
  myGraph.vertices.foreach(print(_))

}
