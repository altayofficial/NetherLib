<?php

/*
 *
 *      _    _ _
 *     / \  | | |_ __ _ _   _
 *    / _ \ | | __/ _` | | | |
 *   / ___ \| | || (_| | |_| |
 *  /_/   \_\_|\__\__,_|\__, |
 *                       |___/
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Original work by the PocketMine Team.
 * https://www.pocketmine.net/
 *
 * @author Altay Team
 * @link https://github.com/altayofficial
 */

declare(strict_types=1);

namespace altay\netherlib\discovery;

use function count;
use function microtime;
use function random_int;
use const PHP_INT_MAX;
use const PHP_INT_MIN;

/**
 * Bookkeeping for reliable discovery messages, both ways. Incoming ones are remembered for a while so
 * a repeat gets acknowledged again without being handled twice, and outgoing ones are kept until
 * the peer acknowledges them or we give up repeating.
 *
 * Peers that never sent us a reliable packet get plain messages, since there's no telling whether
 * an older client understands the reliable one.
 */
final class ReliableDelivery{

	/** How long a received packet is remembered for spotting its repeats */
	private const RECEIVED_TTL = 15;
	/** Sender IDs are whatever the datagram claims, so this can't grow without a bound */
	private const MAX_RECEIVED = 1024;
	private const MAX_OUTGOING = 256;
	private const MAX_PEERS = 256;
	private const RETRANSMIT_INTERVAL = 1.0;
	private const MAX_ATTEMPTS = 5;
	/** How long a peer counts as speaking reliable after the last reliable packet from it */
	private const PEER_TTL = 300;

	private int $nonce;
	private int $sequence = 0;

	/** @var array<string, int> */
	private array $received = [];
	/** @var array<string, array{datagram: string, address: string, port: int, sentAt: float, attempts: int}> */
	private array $outgoing = [];
	/** @var array<int, int> */
	private array $peers = [];

	public function __construct(){
		//stays the same for as long as we run, the sequence is what tells our packets apart
		$this->nonce = random_int(PHP_INT_MIN, PHP_INT_MAX);
	}

	private static function key(int $networkId, int $nonce, int $sequence) : string{
		return "$networkId/$nonce/$sequence";
	}

	/**
	 * Records a reliable packet from a peer and marks the peer as one that speaks reliable.
	 *
	 * @return bool false if the packet was seen before, it still needs acknowledging but not handling
	 */
	public function receive(int $senderId, DiscoveryReliablePacket $packet, int $now) : bool{
		$this->markPeer($senderId, $now);
		$key = self::key($senderId, $packet->nonce, $packet->sequence);
		if(isset($this->received[$key])){
			return false;
		}
		if(count($this->received) >= self::MAX_RECEIVED){
			$this->expire($now);
		}
		//hmm, still full means someone is flooding us. Handling a repeat twice is harmless, refusing a
		//real signal isn't, so just stop remembering until it drains
		if(count($this->received) < self::MAX_RECEIVED){
			$this->received[$key] = $now;
		}
		return true;
	}

	private function markPeer(int $networkId, int $now) : void{
		if(!isset($this->peers[$networkId]) && count($this->peers) >= self::MAX_PEERS){
			$this->expire($now);
			if(count($this->peers) >= self::MAX_PEERS){
				return;
			}
		}
		$this->peers[$networkId] = $now;
	}

	public function speaksReliable(int $networkId) : bool{
		return isset($this->peers[$networkId]);
	}

	/**
	 * Wraps a message for the given peer, taking the next sequence number.
	 */
	public function wrap(DiscoveryMessagePacket $message, int $localNetworkId) : DiscoveryReliablePacket{
		return new DiscoveryReliablePacket($message->recipientId, $this->nonce, ++$this->sequence, $localNetworkId, $message);
	}

	/**
	 * Keeps an already sent packet around for retransmit() until it's acknowledged. When too much is
	 * in flight it just goes out once, like a plain message would.
	 */
	public function track(DiscoveryReliablePacket $packet, string $datagram, string $address, int $port) : void{
		if(count($this->outgoing) >= self::MAX_OUTGOING){
			return;
		}
		$this->outgoing[self::key($packet->recipientId, $packet->nonce, $packet->sequence)] = [
			"datagram" => $datagram,
			"address" => $address,
			"port" => $port,
			"sentAt" => microtime(true),
			"attempts" => 1
		];
	}

	/**
	 * @return bool false if nothing was waiting on this acknowledgement
	 */
	public function acknowledge(int $senderId, DiscoveryAcknowledgementPacket $packet) : bool{
		$key = self::key($senderId, $packet->nonce, $packet->sequence);
		if(!isset($this->outgoing[$key])){
			return false;
		}
		unset($this->outgoing[$key]);
		return true;
	}

	/**
	 * Picks the packets due for another send and counts the attempt. Ones that ran out of attempts are
	 * dropped, the negotiation repeats its own signals on top of this anyway.
	 *
	 * @return list<array{string, string, int}> datagram, address and port of each
	 */
	public function retransmit(float $now) : array{
		$due = [];
		foreach($this->outgoing as $key => $entry){
			if($now - $entry["sentAt"] < self::RETRANSMIT_INTERVAL){
				continue;
			}
			if($entry["attempts"] >= self::MAX_ATTEMPTS){
				unset($this->outgoing[$key]);
				continue;
			}
			$this->outgoing[$key]["sentAt"] = $now;
			$this->outgoing[$key]["attempts"]++;
			$due[] = [$entry["datagram"], $entry["address"], $entry["port"]];
		}
		return $due;
	}

	public function expire(int $now) : void{
		foreach($this->received as $key => $seenAt){
			if($now - $seenAt >= self::RECEIVED_TTL){
				unset($this->received[$key]);
			}
		}
		foreach($this->peers as $networkId => $seenAt){
			if($now - $seenAt >= self::PEER_TTL){
				unset($this->peers[$networkId]);
			}
		}
	}

	public function getOutgoingCount() : int{
		return count($this->outgoing);
	}
}
