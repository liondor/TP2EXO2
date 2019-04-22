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
  def launchAttack (s : String) : Int =
  {
    attack
  }

  override def toString: String = s"id : $id hp : $hp attack : $attack armor : $armure  vitesse : $vitesse position : $position porté Maximale : $porteMax\n"
}
/* La caractéristique attaque ne sert  à rien pour le solar*/
case class Solar(id: String= "Solar", var  hp : Int=363,  attack : Int =18,  armure: Int=44, vitesse : Int=50, var position: Int= scala.util.Random.nextInt(300), porteMax : Int = 110) extends node
{
   override def launchAttack(arme : String) : Int =
  {
    if(arme.contentEquals("Sword"))
       {
         val r = scala.util.Random
         var res = 18+ r.nextInt(5)+r.nextInt(5)+r.nextInt(5)+3
         res
       }
    else
      {
        val r = scala.util.Random
        var res = r.nextInt(5)+r.nextInt(5)+2+14
        res
      }

  }

}//scala.util.Random.nextInt(500)
case class Warlord( id: String="Warlord", var hp: Int =141,attack : Int =10,armure: Int=27,vitesse: Int = 30, var position: Int= scala.util.Random.nextInt(300), porteMax : Int = 10) extends node
{
  override def launchAttack() : Int =
  {
    val r = scala.util.Random
    var res = attack+ r.nextInt(7)+1
    res
  }

}
object test extends App {
  def applyAction(vId :VertexId, perso : node , typeAction : String,dommage : Int) : node =
  {
    if(typeAction.contentEquals("dmg")) {

      perso.hp = perso.hp - dommage
      perso
    }
    else {

      perso.position = perso.position + dommage
      perso
    }
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
          val posSolar = container.find(p => p._1 == triplet.srcId)
          val posEnnemi = container.find(p => p._1 == triplet.dstId)
          if (posEnnemi.last._2.hp > 0) {
            triplet.sendToSrc(scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position))
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

      var damage = newGraph.aggregateMessages[Tuple2[String,Int]](
        triplet => {
          var container = myGraph.vertices.collect()
          val posSolar = container.find(p => p._1 == triplet.srcId)
          val posEnnemi = container.find(p => p._1 == triplet.dstId)

          if (scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position ) == position)
          {
            var ciblage =35 - nbrAttaque*5

            if(scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position )<= triplet.srcAttr.porteMax)
            {
              if (scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position) <= 10)
              {
                var ciblage =35 - nbrAttaque*5
                if(ciblage>=posEnnemi.last._2.armure) {
                  var degat = triplet.srcAttr.launchAttack("Sword")
                  triplet.sendToDst("dmg", degat)
                  print("Bim dans les dents ! Attaque sur un " + triplet.dstAttr.id + " avec la greatsword, le Solar lui inflige " + degat + " points de dommages\n")
                }
                else{
                  triplet.sendToDst("dmg", 0)
                  print("Un bon swing ! ....Dans le vent. Le Solar a râté son attaque à l'épée !\n")

                }
              }
              else
              {
                var ciblage =31 - nbrAttaque*5
                if(ciblage>=posEnnemi.last._2.armure) {

                  var degat = triplet.srcAttr.launchAttack("Arc")
                  triplet.sendToDst("dmg", degat)
                  print("Attaque sur un " + triplet.dstAttr.id + " avec l'arc! Le Solar lui inflige " + degat + " points de dommages (itération n°"+nbrAttaque+")\n")
                }
                else {
                  triplet.sendToDst("dmg", 0)
                  print("Le Solar a râté son attaque à l'arc ! (Itération n°" + nbrAttaque + ") \n")
                }
              }
            }
            else
              {
                if(nbrAttaque==0)
                  {
                    print("On bouge vers les ennemis")
                    if (posSolar.last._2.position - posEnnemi.last._2.position  >0) {
                      if (scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position ) < posSolar.last._2.vitesse) {
                      //var mvmt = -triplet.srcAttr.vitesse
                      var mvmt = -scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position )
                      triplet.sendToSrc("mvmt", mvmt)
                      }
                      else{
                        var mvmt = -triplet.srcAttr.vitesse
                        triplet.sendToSrc("mvmt", mvmt)
                      }

                    }
                    else{
                      if (scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position ) < posSolar.last._2.vitesse) {
                        var mvmt = scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position )
                        triplet.sendToSrc("mvmt", mvmt)
                      }
                        var mvmt = triplet.srcAttr.vitesse
                      triplet.sendToSrc("mvmt", mvmt)


                    }

                  }
                else{
                  triplet.sendToSrc("mvmt", 0)

                }

              }
          }
        },

        (a, b) => {
          a
        }

      )
      //newGraph.vertices.foreach(print(_))
      newGraph = newGraph.joinVertices(damage)(
        (vid, personnage, msg) => applyAction(vid, personnage,msg._1, msg._2))
      val action = damage.collect().last._2._1
       if(action.contentEquals("mvmt"))
        {
            nbrAttaque += 4
          }

        else
      {
        nbrAttaque += 1

      }
      newGraph = newGraph.subgraph(vpred = (id, attr) => attr.hp > 0)
      newGraph.vertices.collect()
      myVertices.collect()
      print("\n")
    }

    /* On regarde ce que les ennemis vont faire !*/
  //  print("----- Tour des ennemis----\n")
    val damageEnemy = newGraph.aggregateMessages[Tuple2[String,Int]](
      triplet => {
        var container = myGraph.vertices.collect()
        val posSolar = container.find(p => p._1 == triplet.srcId)
        val posEnnemi = container.find(p => p._1 == triplet.dstId)

        if (posEnnemi.last._2.hp > 0) {

          if(scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position )<= triplet.dstAttr.porteMax)
          {
            var degat = posEnnemi.last._2.launchAttack()
            triplet.sendToSrc("dmg",degat)
            print("Un " + triplet.dstAttr.id + " attaque le Solar !  Il lui inflige " + degat + " points de dommages\n")

          }
          else
          {
              print("Un "+triplet.dstAttr.id+" s'approche du Solar !\n")
              if (posEnnemi.last._2.position - posSolar.last._2.position  >0) {
                //Si je peux me coller au Solar, je le fait !
                if (scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position ) < posEnnemi.last._2.vitesse) {
                  var mvmt = -scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position )
                  triplet.sendToDst("mvmt", mvmt)
                }
                  //Sinon je me déplacec dans sa direction
                else{
                  var mvmt = -triplet.dstAttr.vitesse
                  triplet.sendToDst("mvmt", mvmt)
                }

              }
              else{
                  if (scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position ) < posEnnemi.last._2.vitesse) {
                    var mvmt = scala.math.abs(posSolar.last._2.position - posEnnemi.last._2.position )
                    triplet.sendToDst("mvmt", mvmt)
                  }
                else{
                    var mvmt = triplet.dstAttr.vitesse
                    triplet.sendToDst("mvmt", mvmt)
                  }

              }

            }


          }

      },
      (a, b) => {
        ( a._1, a._2+b._2)

      }
    )

    print("\n")
    //print("----- Fin du tour----\n")
    newGraph = newGraph.joinVertices(damageEnemy)(
      (vid, personnage, msg) => applyAction(vid, personnage,msg._1, msg._2))


    newGraph = newGraph.subgraph(vpred = (id, attr) => attr.hp > 0)

    //newGraph.vertices.map(i => {
     // i._2.hp += i._2.

    //})
    //print("----- État actuel  ----\n")

    newGraph.vertices.foreach(print(_))
    print("\n")
  }
}
