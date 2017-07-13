package nyctaxi

import com.fasterxml.jackson.annotation.JsonRawValue

/**
  * Created by alons on 7/12/17.
  */
case class Feature (
                   @JsonRawValue
                  var rawJson:String ){
      def this()=this(null)
}
