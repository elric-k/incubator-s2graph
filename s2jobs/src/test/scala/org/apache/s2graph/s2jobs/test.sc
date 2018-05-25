import scala.reflect.api
import scala.reflect.runtime.universe._

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.currentMirror

val toolbox = currentMirror.mkToolBox()
//val fnStr = "(a:Int, b:Int) => { a+b }"
//val f = tb.eval(tb.parse(fnStr)).asInstanceOf[(Any, Any) => Any]
//f(2,1)
//

def createTypeTag(tp: String): TypeTag[_] = {
  val ttagCall = s"scala.reflect.runtime.universe.typeTag[$tp]"
  val tpe = toolbox.typecheck(toolbox.parse(ttagCall), toolbox.TYPEmode).tpe.resultType.typeArgs.head

  TypeTag(currentMirror, new reflect.api.TypeCreator {
    def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
      assert(m eq currentMirror, s"TypeTag[$tpe] defined in $currentMirror cannot be migrated to $m.")
      tpe.asInstanceOf[U#Type]
    }
  })
}

def stringToTypeTag[A](name: String): TypeTag[A] = {
  val c = Class.forName(name)  // obtain java.lang.Class object from a string
  val mirror = runtimeMirror(c.getClassLoader)  // obtain runtime mirror
  val sym = mirror.staticClass(name)  // obtain class symbol for `c`
  val tpe = sym.selfType  // obtain type object for `c`

  // create a type tag which contains above type object
  TypeTag(mirror, new api.TypeCreator {
    def apply[U <: api.Universe with Singleton](m: api.Mirror[U]) =
      if (m eq mirror) tpe.asInstanceOf[U # Type]
      else throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
  })
}
//def createTypeTag(tp: String): TypeTag[_] = {
//     val ttree = tb.parse(s"scala.reflect.runtime.universe.typeTag[$tp]")
//     tb.eval(ttree).asInstanceOf[TypeTag[_]]
// }

def cast[A](a: Any, tt: TypeTag[A]): A = a.asInstanceOf[A]

//Integer
val t = stringToTypeTag[Int]("java.lang.Integer")
val tt = createTypeTag("Int")
t.tpe
val a:Any = 1
cast(1, tt)

val code = "(a:Int, b:Int) => {a+b}"

def custom2[RT](code:String, tt: TypeTag[RT]) = {
  new Function2[Any, Any, RT] with Serializable {

    import scala.reflect.runtime.universe._
    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox

    lazy val toolbox = currentMirror.mkToolBox()
    lazy val func = {
      println("reflected function") // triggered at every worker
      toolbox.eval(toolbox.parse(code)).asInstanceOf[(Any, Any) => RT]
    }

    override def apply(v1: Any, v2: Any): RT = func(v1, v2)
  }
}

val typeStr = "java.lang.String"

Class.forName(typeStr)

//ss.udf.register("add2", udf(func2))


//import scala.reflect.runtime.universe
//import scala.tools.reflect.ToolBox
//
//val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
//
//val code =
//  """
//    object FunctionWrapper {
//      import java.sql.Timestamp
//
//      def apply(datetime : Timestamp, hours : Int):Timestamp = {
//        new Timestamp(datetime.getTime() + hours * 60 * 60 * 1000 )
//      }
//    }
//  """.stripMargin
////val code = """println("ss")"""
//val tree = tb.parse(code).asInstanceOf[tb.u.ImplDef]
//DefaultValue
////val f1 = tb.eval(tree).asInstanceOf[(Int, Int) => Int]
//val s:Any = "s"
//s.asInstanceOf[String]
//
//
//
//
//val tf = (datetime : Timestamp, hours : Int) => { new Timestamp(datetime.getTime() + hours * 60 * 60 * 1000 ) }
//
//val ts = new Timestamp(System.currentTimeMillis)
//
//tf(ts, 1)
//import org.apache.s2graph.s2jobs.udfs.{GrokUdf, Udf}
//
//val udf = Class.forName("org.apache.s2graph.s2jobs.udfs.GrokUdf").newInstance().asInstanceOf[Udf]
//udf.register(ss, udfOption.name, udfOption.params.getOrElse(Map.empty))
//
//val cnt = 1057055.0
//val itemTotalCnt = 9459211.0
//val dimValTotalCnt = 11394556.0
//val maxCnt = 1057055.0 //21475.0
//
//val itemScore = cnt / itemTotalCnt
//val dimvalScore = cnt / dimValTotalCnt
//
//val maxItemScore = 1
//val maxDimvalScore = maxCnt / dimValTotalCnt
//
//itemScore + dimvalScore / maxDimvalScore
//
//
//def xLogX(x: Long) = if (x == 0) 0.0 else x * Math.log(x)
//
//def entropy2(a: Long, b: Long): Double = xLogX(a + b) - xLogX(a) - xLogX(b)
//
//def entropy4(a: Long, b: Long, c: Long, d: Long): Double = xLogX(a + b + c + d) - xLogX(a) - xLogX(b) - xLogX(c) - xLogX(d)
//
////val dimTotal = 93988805
////val dimValTotal = 11796003
////val itemTotal = 522351
////val itemDimTotal = 522351
//
//val dimTotal = 93988805
//val dimValTotal = 9314596
//val itemTotal = 3
//val itemDimTotal = 3
//
//val k11 = 3
//val k12 = dimValTotal - k11   // 9314593
//val k21 = itemDimTotal - k11  // 0
//val k22 = dimTotal - dimValTotal - itemDimTotal + k11 //84674209
//
////val k11 = 67624
////val k12 = 11326932
////val k21 = 454727
////val k22 = 82139522
//
////val k11 = 14802
//////val k12 = 11781201
////val k12 = 11781201
////val k21 = 507549
////val k22 = 81685253
//
//val rowEntropy = entropy2(k11 + k12, k21 + k22)
//val columnEntropy = entropy2(k11 + k21, k12 + k22)
//val matrixEntropy = entropy4(k11, k12, k21, k22)
//
////  println(s"rowEntropy: $rowEntropy, columnEntropy: $columnEntropy, matrixEntropy: $matrixEntropy")
//2.0 * (rowEntropy + columnEntropy - matrixEntropy)
//
//
//val negative = k12.toDouble / (k12 + k22)
//val positive = k11.toDouble / (k11 + k12)
//
//
//
////LLR = 2 sum(k) (H(k) – H(rowSums(k)) – H(colSums(k)))
////
////where H is Shannon’s entropy, computed as the sum of (k_ij / sum(k)) log (k_ij / sum(k)).
//// In R, this function is defined as
////
////H = function(k) {N = sum(k) ; return (sum(k/N * log(k/N + (k==0)))}.