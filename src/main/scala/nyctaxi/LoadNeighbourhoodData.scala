package nyctaxi

import java.io.{File, FileReader}
import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.openspaces.spatial.shapes.Shape
import org.openspaces.spatial.{ShapeFactory, shapes}

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by alons on 7/9/17.
  */
object LoadNeighbourhoodData {

  def getManhattanNeighbourhoods: mutable.Map[String,Neighbourhood] = {
    val map = mutable.Map.empty[String, Neighbourhood]
    val file = new File("nyc_neighbourhoods.json")
    val mapper: ObjectMapper = new ObjectMapper()

    import com.fasterxml.jackson.databind.DeserializationFeature
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val rootNode: JsonNode = mapper.readTree(file).get("features")

    if (rootNode.isArray) {

      for (n <- rootNode) {

        val neighbourhood: Neighbourhood = new Neighbourhood()

        neighbourhood.shape = getShapeFromJsonNode(n)
        neighbourhood.properties = getPropertiesFromJsonNode(n)

        if (neighbourhood.properties.getProperty("BoroName").contentEquals("Manhattan")) {
          map + (neighbourhood.properties.getProperty("NTAName") -> neighbourhood)
        }
      }
    }

      return map
    }

    def getShapeFromJsonNode(node: JsonNode): mutable.MutableList[Shape]={

      val result: mutable.MutableList[Shape]=new mutable.MutableList[Shape]()
      val coordinates: util.Collection[org.openspaces.spatial.shapes.Point]=new util.ArrayList[shapes.Point]()
      val shapeType: String= node.get("type").asText()
      val coordNode: JsonNode=node.get("geometry").get("coordinates")

      if(shapeType.contentEquals("Polygon")) {
        for (cNode <- coordNode) {

          for(ccNode <- cNode){

            val iterator: Iterator[JsonNode] = ccNode.elements()

            while (iterator.hasNext) {
              val x: Double = iterator.next().asDouble()
              if (iterator.hasNext) {
                val y: Double = iterator.next().asDouble()
                val cord: org.openspaces.spatial.shapes.Point = ShapeFactory.point(x, y)
                //                println("["+x+","+y+"]")
                coordinates.add(cord)
              }
            }
            result += ShapeFactory.polygon(coordinates)
          }
        }
      }
      if(shapeType.contentEquals("MultiPolygon")) {
        for (cNode <- coordNode) {
          for (ccNode <- cNode) {
            for (cccNode <- ccNode) {
              val iterator: Iterator[JsonNode] = cccNode.elements()
              while (iterator.hasNext) {
                val x: Double = iterator.next().asDouble()
                if (iterator.hasNext) {
                  val y: Double = iterator.next().asDouble()
                  val cord: org.openspaces.spatial.shapes.Point = ShapeFactory.point(x, y)
                  //                println("["+x+","+y+"]")
                  coordinates.add(cord)
                }
              }
              result += ShapeFactory.polygon(coordinates)
              coordinates.clear()
            }
          }
        }
      }

      return result

    }

    def getPropertiesFromJsonNode(node: JsonNode): Properties={

      val propsNode: JsonNode = node.get("properties")
      var props: Properties=new Properties()

      for(entry <- propsNode.fields){

        val key: String= entry.getKey
        val value: String=entry.getValue.asText()
        //          println("["+key+","+value+"]")
        props.setProperty(key, value)
      }

      return props
    }

    //    val string="{\"type\":\"Feature\",\"id\":1,\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-73.9760507905698,40.6312841471042],[-73.9771665542679,40.6307548954438],[-73.9769999234971,40.6298797372738],[-73.9768510772105,40.6290968224783],[-73.9766974777461,40.6283628050698],[-73.9765791908336,40.6275814275307],[-73.9765160549069,40.6273027311531],[-73.9764511382828,40.6270164960783],[-73.9762359705636,40.6259845911404],[-73.9772629343846,40.6258610131635],[-73.9771980904413,40.6251102187637],[-73.9771110268827,40.6249564998691],[-73.9769452575888,40.6240609900222],[-73.9768360858429,40.6234892818447],[-73.9767535301789,40.6230215521357],[-73.9765672728723,40.6220031428102],[-73.9769521418502,40.6216382467059],[-73.9770549623761,40.6215407613162],[-73.9753952383902,40.6207782266542],[-73.9768257690581,40.6186840391146],[-73.9775244525444,40.617684779502],[-73.9778515271106,40.6172958454415],[-73.9756309743991,40.6159536291278],[-73.9753747881662,40.6157629608147],[-73.9754441999024,40.6161223288555],[-73.9754824027154,40.6163201636822],[-73.974444717012,40.6164298820579],[-73.9733602257552,40.6165499539539],[-73.9729047013909,40.6141612065885],[-73.9739946269133,40.6140381323842],[-73.9751732026353,40.6147324241179],[-73.9749740792352,40.6136763482082],[-73.9748943901032,40.6131946063585],[-73.9747780122118,40.6126467127678],[-73.9752097267571,40.6128934960896],[-73.9774095979934,40.6142263510495],[-73.9796396519266,40.6155727854739],[-73.9800407379246,40.615815167947],[-73.9808805624775,40.616322673138],[-73.9818696174241,40.6169203650364],[-73.9840693534426,40.6182489779194],[-73.9862766836595,40.6195748553897],[-73.9856950146526,40.6201397533789],[-73.9878523925214,40.6214432715525],[-73.9900949984916,40.6227971926424],[-73.99255117502,40.6242825082844],[-73.9939834108584,40.6251496386759],[-73.9943264136196,40.6253511484503],[-73.9948942452892,40.6254700338481],[-73.9947362964268,40.6256229802954],[-73.9946511693536,40.6257054189326],[-73.995079588454,40.6258149775548],[-73.9967899156773,40.6268342912707],[-73.9968792239126,40.6265390223592],[-73.9970177768712,40.6260698551505],[-73.9972421108136,40.6252969265683],[-73.9981175840122,40.6258239462227],[-74.0003151362792,40.6271528223909],[-74.0025159903588,40.6284824495438],[-74.0022293761833,40.6287594096894],[-74.0019315565538,40.6290410080027],[-74.0030629828535,40.6297234156103],[-74.0036874152191,40.6301000212857],[-74.0041387226147,40.6303722061321],[-74.0063401502436,40.6317018671631],[-74.0070244688213,40.6321182360137],[-74.0068082224122,40.6322586318078],[-74.0066205934426,40.632383699193],[-74.0063075991434,40.6325923291178],[-74.0055663474715,40.6330914171937],[-74.0073578108183,40.6341718681679],[-74.0067526712103,40.6347530275938],[-74.0064192907805,40.6350730915942],[-74.0061707125885,40.6353117431103],[-74.0055875737634,40.6358744703585],[-74.005004908848,40.6364334766329],[-74.0044219272224,40.6369932279474],[-74.004112484189,40.6372912026574],[-74.0038393741748,40.6375541735836],[-74.0032541744095,40.6381125038692],[-74.0026735863878,40.6386752524471],[-74.0023708776843,40.6389659198587],[-74.0020921971601,40.6392334964153],[-74.0015091552022,40.6397932043106],[-74.0009268602378,40.6403541443966],[-74.0006622242406,40.6406075516282],[-74.0003426630167,40.6409135586713],[-73.9997583506462,40.6414753930748],[-73.999177089234,40.6420336847879],[-73.996975015354,40.6407023471692],[-73.9955117783607,40.6398179095822],[-73.9947968359321,40.6402967299556],[-73.9943326816783,40.6406085306016],[-73.9940783604289,40.6407786444513],[-73.993363976848,40.6412560417877],[-73.9926460005131,40.6417365338756],[-73.991929563492,40.6422131471339],[-73.991214213337,40.6426941336726],[-73.9904977801622,40.6431726844747],[-73.9897803418762,40.6436531301367],[-73.9890601628697,40.6441274845632],[-73.9883513054438,40.6445704875231],[-73.9875008443504,40.644053753919],[-73.9867322438464,40.6435822372057],[-73.9858978827732,40.6430758387584],[-73.985096633962,40.6425812992004],[-73.9845886322156,40.6422702478885],[-73.9843043687222,40.6420928063844],[-73.9833177214577,40.6414869260397],[-73.9829956292387,40.6413332400368],[-73.9824459788592,40.6415468688108],[-73.9820446541955,40.6417158253016],[-73.9812338413175,40.642053157281],[-73.9804220142833,40.6423934703916],[-73.980075720791,40.6405479429837],[-73.9800083658258,40.6402170808277],[-73.9799112671232,40.6396946008856],[-73.9798750387037,40.6395264488926],[-73.9796784851557,40.6384730425602],[-73.9795558471337,40.6378275023104],[-73.9794773563618,40.6374396065307],[-73.9793878825889,40.636940405939],[-73.9792873527091,40.6364224365445],[-73.9791133226045,40.6354486286075],[-73.9780230243262,40.6355609451335],[-73.9768900459946,40.6356831043202],[-73.9766379945809,40.6343620633121],[-73.976552075801,40.6339117511],[-73.9764348560386,40.63329736443],[-73.9760507905698,40.6312841471042]]]},\"properties\":{\"OBJECTID\":1,\"BoroCode\":3,\"BoroName\":\"Brooklyn\",\"CountyFIPS\":\"047\",\"NTACode\":\"BK88\",\"NTAName\":\"Borough Park\",\"Shape__Area\":0.000534040202917259,\"Shape__Length\":0.12354783848794}}"
    //    val shape: Shape= ShapeFactory.parse(string, ShapeFormat.GEOJSON)


}
