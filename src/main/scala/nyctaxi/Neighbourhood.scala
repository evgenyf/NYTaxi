package nyctaxi

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.openspaces.spatial.shapes.Shape

import scala.collection.mutable

/**
  * Created by alons on 7/9/17.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Neighbourhood (
                           var shape : mutable.MutableList[Shape],
                           var properties : Properties

                         ) {
  def this()=this(null,null)
}
