import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}
import org.apache.spark.rdd.RDD


trait node {
val id: String
var  hp: Int
  val attack: Int
val armure: Int
val vitesse : Int
  var position :Int
  val porteMax : Int
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
    var res = attack+ r.nextInt(5)+r.nextInt(5)+r.nextInt(5)+3
    res
  }

}
case class Warlord( id: String="Warlord", var hp: Int =141,attack : Int =10,armure: Int=27,vitesse: Int = 30, var position: Int= scala.util.Random.nextInt(500), porteMax : Int = 10) extends node
{
  override def launchAttack() : Int =
  {
    val r = scala.util.Random
    var res = attack+ r.nextInt(7)+1
    res
  }

}
object test extends App {
  def applyDamage(vId :VertexId, perso : node ,dommage : Int) : node =
  {

    perso.hp= perso.hp-dommage
    perso

  }

  import org.apache.spark.SparkConf
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


  /*--------------  Set up (pas sur pour le .cache)        -----------------*/
  var myVertices : RDD[(VertexId,node)]= sc.parallelize(Array((1L,Solar()),(2L,Warlord()),(3L,Warlord())))
  myVertices.cache()
  var myEdges = sc.parallelize(Array(Edge(1L,2L,"1"),Edge(1L,3L,"2")))
  var myGraph = Graph(myVertices, myEdges)
  myGraph.cache()


  print("----- Graphe de base ----\n")
  myGraph.vertices.foreach(print(_))
  print("\n")
  var newGraph = myGraph

  /*-------------- Début de la partie   ----------------------------------------*/
  while(true) {
  //  print("----- Tour du Solar ----\n")
    var nbrAttaque: Int = 0
    while (nbrAttaque < 4) {
      /* Premier aggregate pour trouver l'ennemi le plus proche*/
      var messages = newGraph.aggregateMessages[Int](
        triplet => {
          var container = myGraph.vertices.collect()
          var valeur = container.find(p => p._1 == triplet.dstId)
          if (valeur.last._2.hp > 0) {
            triplet.sendToSrc(scala.math.abs(triplet.dstAttr.position - triplet.srcAttr.position))
          }
        },
        {
          (a, b) => {
            if (a < b) {
              a
            }
            else {
              b
            }

          }
        }
      )


      if (messages.isEmpty()) {
        print("----- Fin de la partie ! ----\n")
        print("État final : \n")
        newGraph.vertices.foreach(print(_))
        System.exit(0);
      }
      /* Deuxième  pour l'attaquer si il est assez proche 'pas encore implémenter la porté)*/
      var position = 0
      position = messages.collect().last._2

      var damage = newGraph.aggregateMessages[Int](
        triplet => {
          if (scala.math.abs(triplet.dstAttr.position - triplet.srcAttr.position) == position) {
            var degat = triplet.srcAttr.launchAttack()
            triplet.sendToDst(degat)
            print("Attaque sur un "+triplet.dstAttr.id+", le Solar lui inflige "+degat+" points de dommages\n" )

          }
        },
        (a, b) => {
          a
        }
      )
      //newGraph.vertices.foreach(print(_))
      newGraph = newGraph.joinVertices(damage)(
        (vid, personnage, msg) => applyDamage(vid, personnage, msg))
      nbrAttaque += 1
      newGraph = newGraph.subgraph(vpred = (id, attr) => attr.hp > 0)
      newGraph.vertices.collect()
      myVertices.collect()
      print("\n")
    }

    /* On regarde ce que les ennemis vont faire !*/
  //  print("----- Tour des ennemis----\n")
    val damageEnemy = newGraph.aggregateMessages[Int](
      triplet => {
        var container = myGraph.vertices.collect()
        var valeur = container.find(p => p._1 == triplet.dstId)
        if (valeur.last._2.hp > 0) {
          var degat = triplet.dstAttr.launchAttack()
          triplet.sendToSrc(degat)
          print("Un " + triplet.dstAttr.id + " attaque le Solar !  Il lui inflige " + degat + " points de dommages\n")
        }
      },
      (a, b) => {
        a + b
      }
    )

    print("\n")
    //print("----- Fin du tour----\n")
    newGraph = newGraph.joinVertices(damageEnemy)(
      (vid, personnage, msg) => applyDamage(vid, personnage, msg))


    newGraph = newGraph.subgraph(vpred = (id, attr) => attr.hp > 0)

    //print("----- État actuel  ----\n")

    newGraph.vertices.foreach(print(_))
  }
}
