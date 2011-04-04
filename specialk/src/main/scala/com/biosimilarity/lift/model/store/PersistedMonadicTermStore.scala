// -*- mode: Scala;-*- 
// Filename:    PersistedMonadicTermStore.scala 
// Authors:     lgm                                                    
// Creation:    Fri Mar 18 15:04:22 2011 
// Copyright:   Not supplied 
// Description: 
// ------------------------------------------------------------------------


package com.biosimilarity.lift.model.store

import com.biosimilarity.lift.model.store.xml._
import com.biosimilarity.lift.model.agent._
import com.biosimilarity.lift.model.msg._
import com.biosimilarity.lift.lib._

import scala.concurrent.{Channel => Chan, _}
import scala.concurrent.cpsops._
import scala.util.continuations._ 
import scala.xml._
import scala.collection.MapProxy
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.MutableList

import org.prolog4j._

//import org.exist.storage.DBBroker

import org.xmldb.api.base.{ Resource => XmlDbRrsc, _}
import org.xmldb.api.modules._
import org.xmldb.api._

//import org.exist.util.serializer.SAXSerializer
//import org.exist.util.serializer.SerializerPool

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver

import javax.xml.transform.OutputKeys
import java.util.Properties
import java.net.URI
import java.io.File
import java.io.FileInputStream
import java.io.OutputStreamWriter

trait PersistedTermStoreScope[Namespace,Var,Tag,Value] 
extends MonadicTermStoreScope[Namespace,Var,Tag,Value] {  
  trait PersistenceManifest {    
    self : CnxnXML[Namespace,Var,Tag]
	    with CnxnCtxtInjector[Namespace,Var,Tag] =>

    def db : Database
    def storeUnitStr[Src,Label,Trgt]( cnxn : Cnxn[Src,Label,Trgt] ) : String
    def storeUnitStr : String
    def toFile( ptn : mTT.GetRequest ) : Option[File]
    def query( ptn : mTT.GetRequest ) : Option[String]

    def labelToNS : Option[String => Namespace]
    def textToVar : Option[String => Var]
    def textToTag : Option[String => Tag]        

    def kvNameSpace : Namespace

    def asStoreKey(
      key : mTT.GetRequest
    ) : mTT.GetRequest with Factual

    def asStoreValue(
      rsrc : mTT.Resource
    ) : CnxnCtxtLeaf[Namespace,Var,Tag]    
    
    def asStoreRecord(
      key : mTT.GetRequest,
      value : mTT.Resource
    ) : mTT.GetRequest with Factual

    def asCacheValue(
      ccl : CnxnCtxtLabel[Namespace,Var,Tag]
    ) : Value    

    def asCacheValue(
      ltns : String => Namespace,
      ttv : String => Var,
      ttt : String => Tag,
      value : Elem
    ) : Option[Value]

    def asResource(
      key : mTT.GetRequest, // must have the pattern to determine bindings
      value : Elem
    ) : Option[mTT.Resource]
  }

  trait PersistenceManifestTrampoline {
    def persistenceManifest : Option[PersistenceManifest]

    def storeUnitStr[Src,Label,Trgt](
      cnxn : Cnxn[Src,Label,Trgt]
    ) : Option[String] = {
      for( pd <- persistenceManifest ) 
	yield {
	  pd.storeUnitStr( cnxn )
	}
    }

    def storeUnitStr : Option[String] = {
      for( pd <- persistenceManifest ) 
	yield {
	  pd.storeUnitStr
	}
    }

    def query( ptn : mTT.GetRequest ) : Option[String] = {
      for( pd <- persistenceManifest; qry <- pd.query( ptn ) ) 
	yield {
	  qry
	}
    }

    def labelToNS : Option[String => Namespace] = {
      for( pd <- persistenceManifest; ltns <- pd.labelToNS ) 
	yield {
	  ltns
	}
    }
    def textToVar : Option[String => Var] = {
      for( pd <- persistenceManifest; ttv <- pd.textToVar ) 
	yield {
	  ttv
	}
    }
    def textToTag : Option[String => Tag] = {
      for( pd <- persistenceManifest; ttt <- pd.textToTag ) 
	yield {
	  ttt
	}
    }

    def kvNameSpace : Option[Namespace] = {
      for( pd <- persistenceManifest )
	yield { pd.kvNameSpace }
    }
    def asStoreValue(
      rsrc : mTT.Resource
    ) : Option[CnxnCtxtLeaf[Namespace,Var,Tag]] = {
      for( pd <- persistenceManifest ) 
	yield { pd.asStoreValue( rsrc ) }
    }
    def asStoreKey(
      key : mTT.GetRequest
    ) : Option[mTT.GetRequest with Factual] = {
      for( pd <- persistenceManifest )
	yield { pd.asStoreKey( key ) }
    }

    def asStoreRecord(
      key : mTT.GetRequest,
      value : mTT.Resource
    ) : Option[mTT.GetRequest with Factual] = {
      for( pd <- persistenceManifest )
	yield { pd.asStoreRecord( key, value ) }
    }

    def asResource(
      key : mTT.GetRequest, // must have the pattern to determine bindings
      value : Elem
    ) : Option[mTT.Resource] = {
      for( pd <- persistenceManifest; rsrc <- pd.asResource( key, value ) )
	yield { rsrc }
    }

    def asCacheValue(
      ccl : CnxnCtxtLabel[Namespace,Var,Tag]
    ) : Option[Value] = {
      for( pd <- persistenceManifest )
	yield { pd.asCacheValue( ccl ) }
    }

    def asCacheValue(
      ltns : String => Namespace,
      ttv : String => Var,
      ttt : String => Tag,
      value : Elem
    ) : Option[Value] = {
      for(
	pd <- persistenceManifest;
	rsrc <- pd.asCacheValue( ltns, ttv, ttt, value )
      )	yield { rsrc }
    }
  }

  abstract class XMLDBManifest(
    override val db : Database
  ) extends PersistenceManifest 
    with CnxnXQuery[Namespace,Var,Tag]
  with CnxnXML[Namespace,Var,Tag]
  with CnxnCtxtInjector[Namespace,Var,Tag]
  with UUIDOps {
    def asStoreKey(
      key : mTT.GetRequest
    ) : mTT.GetRequest with Factual = {
      key match {
	case leaf : CnxnCtxtLeaf[Namespace,Var,Tag] =>
	  leaf
	case branch : CnxnCtxtBranch[Namespace,Var,Tag] =>
	  branch
      }
    }

    def asStoreRecord(
      key : mTT.GetRequest,
      value : mTT.Resource
    ) : mTT.GetRequest with Factual = {
      new CnxnCtxtBranch[Namespace,Var,Tag](
	kvNameSpace,
	List( asStoreKey( key ), asStoreValue( value ) )
      )
    }

    def asCacheValue(
      ccl : CnxnCtxtLabel[Namespace,Var,Tag]
    ) : Value    

    def asCacheValue(
      ltns : String => Namespace,
      ttv : String => Var,
      ttt : String => Tag,
      value : Elem
    ) : Option[Value] = {
      fromXML( ltns, ttv, ttt )( value ) match {
	case Some( CnxnCtxtBranch( ns, k :: v :: Nil ) ) => {
	  val vale : Value =
	    asCacheValue(	      
	      v.asInstanceOf[CnxnCtxtLabel[Namespace,Var,Tag]]
	    )
	  if ( kvNameSpace.equals( ns ) ) {	    
	    Some( vale )
	  }
	  else {	    
	    None
	  }
	}
	case v@_ => {
	  None
	}
      }
    }

    def asResource(
      key : mTT.GetRequest, // must have the pattern to determine bindings
      value : Elem
    ) : Option[mTT.Resource] = {
      for(
	ltns <- labelToNS;
	ttv <- textToVar;
	ttt <- textToTag;
	vCCL <- asCacheValue( ltns, ttv, ttt, value )	
      ) yield {
	// BUGBUG -- LGM need to return the Solution
	// Currently the PersistenceManifest has no access to the
	// unification machinery
	mTT.RBound( 
	  Some( mTT.Ground( vCCL ) ),
	  None
	)
      }
    }

    override def query(
      ptn : mTT.GetRequest
    ) : Option[String] = {
      for( ttv <- textToVar )
	yield {
	  xqQuery(
	    new CnxnCtxtBranch[Namespace,Var,Tag](
	      kvNameSpace,
	      List(
		asCCL( ptn ),
		new CnxnCtxtLeaf[Namespace,Var,Tag](
		  Right(
		    ttv( "VisForValueVariableUniqueness" )
		  )
		)
	      )
	    )
	  )
	}
    }

    override def toFile(
      ptn : mTT.GetRequest
    ) : Option[File] = {
      // TBD
      None
    }    

  }
  object XMLDBManifest {
    def unapply(
      ed : XMLDBManifest
    ) : Option[( Database )] = {
      Some( ( ed.db ) )
    }
  }  
  
  abstract class PersistedMonadicGeneratorJunction(
    override val name : URI,
    override val acquaintances : Seq[URI]
  ) extends DistributedMonadicGeneratorJunction(
    name,
    acquaintances
  ) with PersistenceManifestTrampoline {    
    override def asCacheValue(
      ltns : String => Namespace,
      ttv : String => Var,
      ttt : String => Tag,
      value : Elem
    ) : Option[Value] = {      
      valueStorageType match {
	case "CnxnCtxtLabel" => {
	  fromXML( ltns, ttv, ttt )( value ) match {
	    case Some( CnxnCtxtBranch( ns, k :: v :: Nil ) ) => {
	      for(
		vale <-
		asCacheValue(	      
		  v.asInstanceOf[CnxnCtxtLabel[Namespace,Var,Tag]]
		);
		if ( kvNameSpace.equals( ns ) )
	      ) yield { vale }
	    }
	    case v@_ => {
	      None
	    }
	  }
	}
	case "XStream" => {
	  fromXML( ltns, ttv )( value ) match {
	    case Some( CnxnCtxtBranch( ns, k :: v :: Nil ) ) => {
	      v match {
		case CnxnCtxtLeaf( Left( t ) ) => {
		  Some(
		    new XStream(
		      new JettisonMappedXmlDriver
		    ).fromXML( t ).asInstanceOf[Value]
		  )
		}
		case _ => None
	      }	      
	    }
	    case v@_ => {
	      None
	    }
	  }	  
	}
	case _ => {
	  throw new Exception( "unexpected value storage type" )
	}
      }      
    }

    def putInStore(
      persist : Option[PersistenceManifest],
      channels : Map[mTT.GetRequest,mTT.Resource],
      ptn : mTT.GetRequest,
      wtr : Option[mTT.GetRequest],
      rsrc : mTT.Resource,
      collName : Option[String]
    ) : Unit = {
      persist match {
	case None => {
	  channels( wtr.getOrElse( ptn ) ) = rsrc	  
	}
	case Some( pd ) => {
	  tweet( "accessing db : " + pd.db )
	  // remove this line to force to db on get
	  channels( wtr.getOrElse( ptn ) ) = rsrc	  
	  spawn {
	    for(
	      rcrd <- asStoreRecord( ptn, rsrc );
	      sus <- storeUnitStr
	    ) {
	      tweet(
		(
		  "storing to db : " + pd.db
		  + " pair : " + rcrd
		  + " in coll : " + sus
		)
	      )
	      store(
		collName.getOrElse(
		  sus
		)
	      )( rcrd )
	    }
	  }
	}
      }
    }

    def putPlaces( persist : Option[PersistenceManifest] )(
      channels : Map[mTT.GetRequest,mTT.Resource],
      registered : Map[mTT.GetRequest,List[RK]],
      ptn : mTT.GetRequest,
      rsrc : mTT.Resource,
      collName : Option[String]
    ) : Generator[PlaceInstance,Unit,Unit] = {    
      Generator {
	k : ( PlaceInstance => Unit @suspendable ) => 
	  // Are there outstanding waiters at this pattern?    
	  val map =
	    Right[
	      Map[mTT.GetRequest,mTT.Resource],
	      Map[mTT.GetRequest,List[RK]]
	    ]( registered )
	  val waitlist = locations( map, ptn )

	  waitlist match {
	    // Yes!
	    case waiter :: waiters => {
	      tweet( "found waiters waiting for a value at " + ptn )
	      val itr = waitlist.toList.iterator	    
	      while( itr.hasNext ) {
		k( itr.next )
	      }
	    }
	    // No...
	    case Nil => {
	      // Store the rsrc at a representative of the ptn
	      tweet( "no waiters waiting for a value at " + ptn )
	      //channels( representative( ptn ) ) = rsrc
	      putInStore(
		persist, channels, ptn, None, rsrc, collName
	      )
	    }
	  }
      }
    }
    
    def mput( persist : Option[PersistenceManifest] )(
      channels : Map[mTT.GetRequest,mTT.Resource],
      registered : Map[mTT.GetRequest,List[RK]],
      consume : Boolean,
      collName : Option[String]
    )(
      ptn : mTT.GetRequest,
      rsrc : mTT.Resource
    ) : Unit @suspendable = {    
      for(
	placeNRKsNSubst
	<- putPlaces(
	  persist
	)( channels, registered, ptn, rsrc, collName )
      ) {
	val PlaceInstance( wtr, Right( rks ), s ) = placeNRKsNSubst
	tweet( "waiters waiting for a value at " + wtr + " : " + rks )
	rks match {
	  case rk :: rrks => {	
	    if ( consume ) {
	      for( sk <- rks ) {
		spawn {
		  sk( s( rsrc ) )
		}
	      }
	    }
	    else {
	      registered( wtr ) = rrks
	      rk( s( rsrc ) )
	    }
	  }
	  case Nil => {
	    putInStore(
	      persist, channels, ptn, Some( wtr ), rsrc, collName
	    )
	  }
	}
      }
      
    }    
    
    def mget(
      persist : Option[PersistenceManifest],
      ask : dAT.Ask,
      hops : List[URI]
    )(
      channels : Map[mTT.GetRequest,mTT.Resource],
      registered : Map[mTT.GetRequest,List[RK]],
      consume : Boolean,
      collName : Option[String]
    )(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      Generator {	
	rk : ( Option[mTT.Resource] => Unit @suspendable ) =>
	  shift {
	    outerk : ( Unit => Unit ) =>
	      reset {
		for(
		  oV <- mget( channels, registered, consume )( path ) 
		) {
		  oV match {
		    case None => {
		      persist match {
			case None => {
			  forward( ask, hops, path )
			  rk( oV )
			}
			case Some( pd ) => {
			  tweet(
			    "accessing db : " + pd.db
			  )
			  val oQry = query( path )
			  oQry match {
			    case None => {
			      forward( ask, hops, path )
			      rk( oV )
			    }
			    case Some( qry ) => {
			      val xmlCollName =
				collName.getOrElse(
				  storeUnitStr.getOrElse(
				    bail()
				  )
				)
			      getCollection( true )(
				xmlCollName
			      )
			      match {
				case Some( xmlColl ) => {
				  val srvc =
				    getQueryService( xmlColl )(
				      queryServiceType,
				      queryServiceVersion
				    )
				
				  tweet(
				    (
				      "querying db : " + pd.db
				      + " from coll " + xmlCollName
				      + " where " + qry
				    )
				  )
				
				  val rsrcSet =
				    execute( xmlColl )( srvc )( qry )
				
				  if ( rsrcSet.getSize == 0 ) {	
				    tweet(
				      (
					"database "
					+ xmlColl.getName
					+ " had no matching resources."
				      )
				    )
				    forward( ask, hops, path )
				    rk( oV )
				  }
				  else {
				    tweet(
				      (
					"database "
					+ xmlColl.getName
					+ " had "
					+ rsrcSet.getSize
					+ " matching resources."
				      )
				    )
				    // BUGBUG -- LGM need to mput rcrds
				    // and delete rcrd from DB
				  
				    val rsrcIter = rsrcSet.getIterator
				    
				    val rcrds =
				      new MutableList[Option[mTT.Resource]]()
				  
				    while( rsrcIter.hasMoreResources ) {
				      val xrsrc = rsrcIter.nextResource	  
				      
				      val xrsrcCntntStr =
				      xrsrc.getContent.toString

				      tweet( "retrieved " + xrsrcCntntStr )
				      
				      val ersrc : Elem =
					XML.loadString( xrsrcCntntStr )									      
				      
				      rcrds += asResource( path, ersrc )
				    }
				    
				    val rslt = rcrds( 0 )
				    val cacheRcrds = rcrds.drop( 1 )
				    for( cacheRcrd <- cacheRcrds ) {
				      tweet( "caching " + cacheRcrd )
				    }
				    
				    tweet( "returning " + rslt )
				    rk( rslt )
				  }
				}
				case _ => {
				  forward( ask, hops, path )
				  rk( oV )
				}
			      }
			    }			    
			  }
			}		      
		      }
		    }
		    case _ => rk( oV )
		  }
		}
	      }
	  }
      }
    }
    
    override def put(
      ptn : mTT.GetRequest, rsrc : mTT.Resource
    ) = {
      val perD = persistenceManifest
      val xmlCollName = 
	perD match {
	  case None => None
	  case Some( pd ) => Some( pd.storeUnitStr )
	}
      mput( perD )(
	theMeetingPlace, theWaiters, false, xmlCollName
      )( ptn, rsrc )
    }
    override def publish(
      ptn : mTT.GetRequest, rsrc : mTT.Resource
    ) = {
      val perD = persistenceManifest
      val xmlCollName = 
	perD match {
	  case None => None
	  case Some( pd ) => Some( pd.storeUnitStr )
	}
      mput( perD )(
	theChannels, theSubscriptions, true, xmlCollName
      )( ptn, rsrc )
    }

    override def get( hops : List[URI] )(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      val perD = persistenceManifest
      val xmlCollName = 
	perD match {
	  case None => None
	  case Some( pd ) => Some( pd.storeUnitStr )
	}
      mget( perD, dAT.AGet, hops )(
	theMeetingPlace, theWaiters, true, xmlCollName
      )( path )    
    }
    override def get(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      get( Nil )( path )    
    }

    override def fetch( hops : List[URI] )(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      val perD = persistenceManifest
      val xmlCollName = 
	perD match {
	  case None => None
	  case Some( pd ) => Some( pd.storeUnitStr )
	}
      mget( perD, dAT.AFetch, hops )(
	theMeetingPlace, theWaiters, false, xmlCollName
      )( path )    
    }
    override def fetch(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      fetch( Nil )( path )    
    }

    override def subscribe( hops : List[URI] )(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      val perD = persistenceManifest
      val xmlCollName = 
	perD match {
	  case None => None
	  case Some( pd ) => Some( pd.storeUnitStr )
	}
      mget( perD, dAT.ASubscribe, hops )(
	theChannels, theSubscriptions, true, xmlCollName
      )( path )    
    }
    override def subscribe(
      path : CnxnCtxtLabel[Namespace,Var,Tag]
    )
    : Generator[Option[mTT.Resource],Unit,Unit] = {        
      subscribe( Nil )( path )    
    }
  }
}


/* ------------------------------------------------------------------
 * Mostly self-contained object to support unit testing
 * ------------------------------------------------------------------ */ 

object PersistedMonadicTS
 extends PersistedTermStoreScope[String,String,String,String] 
  with UUIDOps {
    import SpecialKURIDefaults._
    import CnxnLeafAndBranch._

    type MTTypes = MonadicTermTypes[String,String,String,String]
    object TheMTT extends MTTypes
    override def protoTermTypes : MTTypes = TheMTT

    type DATypes = DistributedAskTypes
    object TheDAT extends DATypes
    override def protoAskTypes : DATypes = TheDAT
    
    lazy val Mona = new MonadicTermStore()
    def Imma( a : String, b : String )  =
      new DistributedMonadicGeneratorJunction( a, List( b ) )
    
    class PersistedtedStringMGJ(
      val dfStoreUnitStr : String,
      override val name : URI,
      override val acquaintances : Seq[URI]
    ) extends PersistedMonadicGeneratorJunction(
      name, acquaintances
    ) {
      class StringXMLDBManifest(
	override val storeUnitStr : String,
	override val labelToNS : Option[String => String],
	override val textToVar : Option[String => String],
	override val textToTag : Option[String => String]        
      )
      extends XMLDBManifest( database ) {
	override def storeUnitStr[Src,Label,Trgt](
	  cnxn : Cnxn[Src,Label,Trgt]
	) : String = {     
	  cnxn match {
	    case CCnxn( s, l, t ) =>
	      s.toString + l.toString + t.toString
	  }
	}	

	def kvNameSpace : String = "record"

	def asStoreValue(
	  rsrc : mTT.Resource
	) : CnxnCtxtLeaf[String,String,String] = {
	  val blob =
	    new XStream( new JettisonMappedXmlDriver ).toXML( rsrc )
	  //asXML( rsrc )
	  new CnxnCtxtLeaf[String,String,String](
	    Left[String,String]( blob )
	  )
	}

	def asCacheValue(
	  ccl : CnxnCtxtLabel[String,String,String]
	) : String = {
	  asPatternString( ccl )
	}
      
      }

      def persistenceManifest : Option[PersistenceManifest] = {
	val sid = Some( ( s : String ) => s )
	Some(
	  new StringXMLDBManifest( dfStoreUnitStr, sid, sid, sid )
	)
      }
    }
    
    def Pimma( storeUnitStr : String, a : String, b : String )  = {
      new PersistedtedStringMGJ( storeUnitStr, a, List( b ) )
    }

    import scala.collection.immutable.IndexedSeq
        
    type MsgTypes = DTSMSH[String,String,String,String]   
    
    val protoDreqUUID = getUUID()
    val protoDrspUUID = getUUID()    
    
    object MonadicDMsgs extends MsgTypes {
      
      override def protoDreq : DReq = MDGetRequest( aLabel )
      override def protoDrsp : DRsp = MDGetResponse( aLabel, aLabel.toString )
      override def protoJtsreq : JTSReq =
	JustifiedRequest(
	  protoDreqUUID,
	  new URI( "agent", protoDreqUUID.toString, "/invitation", "" ),
	  new URI( "agent", protoDreqUUID.toString, "/invitation", "" ),
	  getUUID(),
	  protoDreq,
	  None
	)
      override def protoJtsrsp : JTSRsp = 
	JustifiedResponse(
	  protoDreqUUID,
	  new URI( "agent", protoDrspUUID.toString, "/invitation", "" ),
	  new URI( "agent", protoDrspUUID.toString, "/invitation", "" ),
	  getUUID(),
	  protoDrsp,
	  None
	)
      override def protoJtsreqorrsp : JTSReqOrRsp =
	Left( protoJtsreq )
    }
    
    override def protoMsgs : MsgTypes = MonadicDMsgs
  }
