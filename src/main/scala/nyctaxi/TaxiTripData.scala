/*
 * Copyright (c) 2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nyctaxi

import java.util.Date

import org.insightedge.scala.annotation._

import scala.beans.BeanProperty

case class TaxiTripData(

                       @BeanProperty
                       @SpaceId(autoGenerate = true)
                       var id: String,

                       @BeanProperty
                       var vendorID : Integer,

                       @BeanProperty
                       var tpepPickupDatetime : Date,

                       @BeanProperty
                       var tpepDropoffDatetime : Date,

                       @BeanProperty
                       var passengerCount : Integer,

                       @BeanProperty
                       var tripDistance : Double,

                       @BeanProperty
                       @SpaceIndex
                       var dropoffHour : Integer,

                       @BeanProperty
                       @SpaceSpatialIndex
                       var pickupLocation: org.openspaces.spatial.shapes.Point
                     ) {

  def this() = this(null, null, null, null, -1, -1, -1, null)
}