/*
 * Created on Apr 26, 2005
 * Created by Alon Rohter
 * Copyright (C) Azureus Software, Inc, All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 */

package com.biglybt.core.peermanager.peerdb;

import java.util.*;

import com.biglybt.core.peer.util.PeerUtils;
import com.biglybt.core.util.AEMonitor;
import com.biglybt.core.util.AENetworkClassifier;
import com.biglybt.core.util.RandomUtils;
import com.biglybt.core.util.SystemTime;
import com.biglybt.core.util.bloom.BloomFilter;
import com.biglybt.core.util.bloom.BloomFilterFactory;


/**
 *
 */
public class PeerDatabase {
  private static final int STARTUP_MIN_REBUILD_WAIT_TIME = 10*1000;
  private static final int STARTUP_MILLIS = 120*1000;

  private static final int MIN_REBUILD_WAIT_TIME = 60*1000;
  private static final int MAX_DISCOVERED_PEERS = 500;

  private static final int BLOOM_ROTATION_PERIOD = 7*60*1000;
  private static final int BLOOM_FILTER_SIZE = 10000;

  private final long start_time = SystemTime.getMonotonousTime();

  private final List<PeerExchangerItem> peer_connections = new ArrayList<>();

  private final TreeSet<PeerItem> discovered_peers =
		  new TreeSet<>(
				  new Comparator<PeerItem>() {
					  @Override
					  public int
					  compare(
							  PeerItem o1,
							  PeerItem o2) {
						  long res = o2.getPriority() - o1.getPriority();

						  if (res == 0) {

							  res = o1.compareTo(o2);
						  }

						  return (res < 0 ? -1 : (res > 0 ? 1 : 0));
					  }
				  });

  private final TreeSet<PeerItem> discovered_peers_non_pub =
		  new TreeSet<>(
				  new Comparator<PeerItem>() {
					  @Override
					  public int
					  compare(
							  PeerItem o1,
							  PeerItem o2) {
						  long res = o2.getPriority() - o1.getPriority();

						  if (res == 0) {

							  res = o1.compareTo(o2);
						  }

						  return (res < 0 ? -1 : (res > 0 ? 1 : 0));
					  }
				  });

  private final AEMonitor map_mon = new AEMonitor( "PeerDatabase" );

  private PeerItem[] cached_peer_popularities = null;
  private int popularity_pos = 0;
  private int popularity_pos_non_pub = 0;
  private long last_rebuild_time 	= Integer.MIN_VALUE;
  private long last_rotation_time 	= Integer.MIN_VALUE;

  private PeerItem self_peer;

  private BloomFilter filter_one = null;
  private BloomFilter filter_two = BloomFilterFactory.createAddOnly( BLOOM_FILTER_SIZE );

  private long 	pex_count_last_time;
  private int	pex_count_last;
  private int 	pex_used_count;

  private int	total_peers_returned;


  protected PeerDatabase() {
    /* nothing */
  }


  /**
   * Register a new peer connection with the database.
   * @param base_peer_item key
   * @return registered connection
   */
  public PeerExchangerItem registerPeerConnection( PeerItem base_peer_item, PeerExchangerItem.Helper helper ) {
    try{  map_mon.enter();
      PeerExchangerItem new_connection = new PeerExchangerItem( this, base_peer_item, helper );

      //update connection adds
      for( PeerExchangerItem old_connection : peer_connections ) {  //go through all existing connections
        PeerItem old_key = old_connection.getBasePeer();

        if( old_connection.getHelper().isSeed() && new_connection.getHelper().isSeed() ) {
          continue;  //dont exchange seed peers to other seeds
        }

        old_connection.notifyAdded( base_peer_item );  //notify existing connection of new one
        new_connection.notifyAdded( old_key );  //notify new connection of existing one for initial exchange
      }

      peer_connections.add( new_connection );
      return new_connection;
    }
    finally{  map_mon.exit();  }
  }


  protected void deregisterPeerConnection( PeerExchangerItem item ) {
	PeerItem base_peer_key = item.getBasePeer();
    try{  map_mon.enter();
      peer_connections.remove( item );

      if ( countConnectionsFor( base_peer_key ) == 0 ){
	      //update connection drops
	      for( PeerExchangerItem old_connection : peer_connections ) {  //go through all remaining connections
	        //dont skip seed2seed drop notification, as the dropped peer may not have been seeding initially
	        old_connection.notifyDropped( base_peer_key );  //notify existing connection of drop
	      }
      }
    }
    finally{  map_mon.exit();  }
  }

  private int
  countConnectionsFor(
	PeerItem	peer )
  {
	  int count = 0;
	  for ( PeerExchangerItem item: peer_connections ){
		  if ( item.getBasePeer().equals( peer )){
			  count++;
		  }
	  }
	  return( count );
  }

  public void
  seedStatusChanged(
	  PeerExchangerItem	item )
  {
	  	// only bother with non-seed -> seed transitions, the opposite are too rate to bother with

	  if ( item.getHelper().isSeed()){

		  try{
			  map_mon.enter();

			  for ( PeerExchangerItem connection : peer_connections ){

				  if ( connection != item && connection.getHelper().isSeed()){

					  // System.out.println( "seedStatusChanged: dropping: originator= " + item.getBasePeer().getAddressString() + ",target=" + connection.getBasePeer().getAddressString());

					  connection.notifyDropped( item.getBasePeer() );

					  item.notifyDropped( connection.getBasePeer() );
				  }
			  }
	      }finally{

	    	  map_mon.exit();
	      }
	  }
  }


  /**
   * Add a potential peer obtained via tracker announce, DHT announce, plugin, etc.
   * @param peer to add
   */
  public void addDiscoveredPeer( PeerItem peer ) {
    try{  map_mon.enter();
      for( PeerExchangerItem connection : peer_connections ) {  //check to make sure we dont already know about this peer
        if( connection.isConnectedToPeer( peer ) )  return;  //we already know about this peer via exchange, so ignore discovery
      }

      if( !discovered_peers.contains( peer ) ) {
        if( discovered_peers.size() >= MAX_DISCOVERED_PEERS ) {
          //remove the lowest priority peer
          PeerItem lowest_priority_peer = (PeerItem)discovered_peers.last();
          if( peer.getPriority() > lowest_priority_peer.getPriority() ) {
            discovered_peers.remove( lowest_priority_peer );
            if ( lowest_priority_peer.getNetwork() != AENetworkClassifier.AT_PUBLIC ){
            	discovered_peers_non_pub.remove( lowest_priority_peer );
            }
          }
          else return;  //ignore discovery, we have better ones
        }

        discovered_peers.add( peer );
        if ( peer.getNetwork() != AENetworkClassifier.AT_PUBLIC ){
        	discovered_peers_non_pub.add( peer );
        }
      }
    }
    finally{  map_mon.exit();  }
  }


  /**
   * Get an optimistic connect peer from the database.
   * @return peer to try connect to
   */
  public PeerItem getNextOptimisticConnectPeer( boolean non_public ) {
    return getNextOptimisticConnectPeer( non_public, 0 );
  }

  private PeerItem getNextOptimisticConnectPeer( boolean non_public, int recursion_count ) {
    long now = SystemTime.getMonotonousTime();
    boolean starting_up = now - start_time < STARTUP_MILLIS;

    PeerItem peer = null;
    boolean discovered_peer = false;
    boolean tried_pex = false;

    //try picking a discovered peer first, as those are usually more recent
    if( ( starting_up || RandomUtils.nextInt( 3 ) != 0 ) ) {
	    try{  map_mon.enter();
	      TreeSet<PeerItem> set = non_public?discovered_peers_non_pub:discovered_peers;

	      if( !set.isEmpty() ) {
	        peer = set.first();
	        set.remove( peer );
	        if ( !non_public && peer.getNetwork() != AENetworkClassifier.AT_PUBLIC ){
	        	discovered_peers_non_pub.remove( peer );
	        }
	        discovered_peer = true;
	      }
	    }
	    finally{  map_mon.exit();  }
    }


    if ( peer == null ){

    	tried_pex = true;

    	peer = getPeerFromPEX( now, starting_up, non_public );
    }

    //to reduce the number of wasted outgoing attempts, we limit how frequently we hand out the same optimistic peer in a given time period
    if( peer != null ) {
    	//check if it's time to rotate the bloom filters

      long diff = now - last_rotation_time;

      if ( diff > BLOOM_ROTATION_PERIOD ) {
        filter_one = filter_two;
        filter_two = BloomFilterFactory.createAddOnly( BLOOM_FILTER_SIZE );
        last_rotation_time = now;
      }

      	//check to see if we've already given this peer out optimistically in the last 5-10min

      boolean	already_recorded = false;

      byte[]	peer_serialisation = peer.getSerialization();

      if ( filter_one.contains( peer_serialisation ) && recursion_count < 100 ) {

    	  	// we've recently given this peer, so recursively find another peer to try

    	  PeerItem next_peer = getNextOptimisticConnectPeer( non_public, recursion_count + 1);

    	  if ( next_peer != null ){

    		  	// we've found a better peer that this one. If the existing peer was discovered (as opposed to PEX)
    		  	// then we'll save it for later as it might come in useful

    		  if ( discovered_peer ){

    		    try{  map_mon.enter();

    		    	discovered_peers.add( peer );

    		    	if ( peer.getNetwork() != AENetworkClassifier.AT_PUBLIC ){

    		    		discovered_peers_non_pub.add( peer );
    		    	}

   			    }finally{
   			    	map_mon.exit();
   			    }
    		  }

    		  peer	= next_peer;

    		  already_recorded	= true;	// this peer's already in the bloom filters as it has been returned
    		  							// by a recursive call
    	  }
      }

      if( !already_recorded ) {  //we've found a suitable peer

        filter_one.add( peer_serialisation );
        filter_two.add( peer_serialisation );
      }
    }

    if ( recursion_count == 0 && peer != null ){

    	total_peers_returned++;
    }

    return peer;
  }

  private PeerItem
  getPeerFromPEX(
	long		now,
	boolean		starting_up,
	boolean		non_public )
  {
	  PeerItem	peer;

	  if( cached_peer_popularities == null || popularity_pos == cached_peer_popularities.length ) {  //rebuild needed
		  cached_peer_popularities = null;  //clear cache
		  long time_since_rebuild = now - last_rebuild_time;
		  //only allow exchange list rebuild every few min, otherwise we'll spam attempts endlessly
		  if( time_since_rebuild > (starting_up?STARTUP_MIN_REBUILD_WAIT_TIME:MIN_REBUILD_WAIT_TIME )) {
			  cached_peer_popularities = getExchangedPeersSortedByLeastPopularFirst();
			  popularity_pos = 0;
			  popularity_pos_non_pub = 0;
			  last_rebuild_time = now;
		  }
	  }

	  if ( cached_peer_popularities != null && cached_peer_popularities.length > 0 ) {

		  if ( non_public ){

			  peer = null;

			  for ( int i=popularity_pos_non_pub; i<cached_peer_popularities.length; i++ ){

				  PeerItem p = cached_peer_popularities[i];

				  if ( p.getNetwork() != AENetworkClassifier.AT_PUBLIC ){

					  peer = p;

					  // we don't want to return the same peer again, so we need to track where we are.
					  // However, we can't easily remove from the array, so we just swap it with the
					  // current position and increment the position.

					  cached_peer_popularities[i] = cached_peer_popularities[popularity_pos_non_pub];
					  cached_peer_popularities[popularity_pos_non_pub] = peer;

					  popularity_pos_non_pub++;

					  break;
				  }
			  }
		  }else{

			  peer = cached_peer_popularities[popularity_pos++];
		  }

		  if ( peer != null ){

			  if ( popularity_pos == cached_peer_popularities.length ){

				  popularity_pos_non_pub = popularity_pos;
			  }

			  pex_used_count++;
			  last_rebuild_time = now;  //ensure rebuild waits min rebuild time after the cache is depleted before trying attempts again
		  }
	  }else{

		  peer = null;
	  }

	  return( peer );
  }

  public int
  getExchangedPeerCount()
  {
	  long	now = SystemTime.getMonotonousTime();

	  if ( now - pex_count_last_time >= 10*1000 ){

		  PeerItem[] peers = getExchangedPeersSortedByLeastPopularFirst();

		  pex_count_last = peers==null?0:peers.length;
		  pex_count_last_time = now;
  	  }

	  return( Math.max( 0, pex_count_last - popularity_pos ));
  }

  public int
  getExchangedPeersUsed()
  {
	 return( pex_used_count );
  }

  private PeerItem[] getExchangedPeersSortedByLeastPopularFirst() {
    HashMap popularity_counts = new HashMap();

    try{  map_mon.enter();
      //count popularity of all known peers
      for( PeerExchangerItem connection : peer_connections ) {
        PeerItem[] peers = connection.getConnectedPeers();

        for( int i=0; i < peers.length; i++ ) {
          PeerItem peer = peers[i];
          Integer count = (Integer)popularity_counts.get( peer );

          if( count == null ) {
            count = new Integer( 1 );
          }
          else {
            count = new Integer( count.intValue() + 1 );
          }

          popularity_counts.put( peer, count );
        }
      }
    }
    finally{  map_mon.exit();  }

    if( popularity_counts.isEmpty() )  return null;

    //now sort by popularity
    Map.Entry[] sorted_entries = new Map.Entry[ popularity_counts.size() ];
    popularity_counts.entrySet().toArray( sorted_entries );

    Arrays.sort( sorted_entries, new Comparator() {
      @Override
      public int compare(Object obj1, Object obj2 ) {
        Map.Entry en1 = (Map.Entry)obj1;
        Map.Entry en2 = (Map.Entry)obj2;
        return ((Integer)en1.getValue()).compareTo( (Integer)en2.getValue() );  //we want least popular in front
      }
    });

    PeerItem[] sorted_peers = new PeerItem[ sorted_entries.length ];

    for( int i=0; i < sorted_entries.length; i++ ) {
      Map.Entry entry = sorted_entries[i];
      sorted_peers[i] = (PeerItem)entry.getKey();
    }

    return sorted_peers;
  }


  public PeerItem
  getSelfPeer()
  {
	  return( self_peer );
  }

  public void
  setSelfPeer(
	PeerItem	_self_peer )
  {
	  self_peer = _self_peer;
  }

  public int
  getDiscoveredPeerCount()
  {
	  return( discovered_peers.size());
  }

  public PeerItem[]
  getDiscoveredPeers()
  {
	  try{
		  map_mon.enter();

		  PeerItem[] res = new PeerItem[discovered_peers.size()];

		  discovered_peers.toArray( res );

		  return( res );

	  }finally{

		  map_mon.exit();
	  }
  }

  public PeerItem[]
  getDiscoveredPeers(
	String	address )
  {
	  try{
		  map_mon.enter();

		  List<PeerItem> res = new ArrayList<>();

		  for ( PeerItem p: discovered_peers ){

			  if ( p.getAddressString().equals( address )){

				  res.add( p );
			  }
		  }

		  return( res.toArray( new PeerItem[res.size()]));

	  }finally{

		  map_mon.exit();
	  }
  }

  //TODO destroy() method?

  public String
  getString()
  {
	  return("pc=" + peer_connections.size() + ",dp=" + discovered_peers.size() + "/" + discovered_peers_non_pub.size());
  }
}
