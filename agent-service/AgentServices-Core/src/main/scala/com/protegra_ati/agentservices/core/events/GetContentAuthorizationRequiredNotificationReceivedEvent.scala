package com.protegra_ati.agentservices.core.events

import com.protegra_ati.agentservices.core.messages.content._

/* User: mtodd
*/

class GetContentAuthorizationRequiredNotificationReceivedEvent(source:GetContentAuthorizationRequiredNotification) extends MessageEvent[GetContentAuthorizationRequiredNotification](source) {
   override def triggerEvent(adapter: MessageEventAdapter) = {
    adapter.getContentAuthorizationRequiredNotificationReceived(this)
  }

}