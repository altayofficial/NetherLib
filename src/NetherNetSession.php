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

namespace altay\netherlib;

use altay\netherlib\rtc\RtcChannel;
use altay\netherlib\rtc\RtcPeer;
use altay\netherlib\sdp\AnswerRewriter;

final class NetherNetSession{

	public const RELIABLE_CHANNEL = "ReliableDataChannel";
	public const UNRELIABLE_CHANNEL = "UnreliableDataChannel";

	//one byte of the advertised maximum is spent on the segment counter
	private const MAX_MESSAGE_SIZE = AnswerRewriter::MAX_MESSAGE_SIZE - 1;
	private const MAX_SEGMENTS = 256;
	private const MAX_PENDING_PACKETS = 16;
	/**
	 * The segment counter alone would allow a peer to stream 255 full-sized messages into the
	 * assembly buffer before the packet is complete, so the assembled size is capped as well. Game
	 * batches stay far below this, it only exists to bound what a peer can make the server hold.
	 */
	private const MAX_PACKET_SIZE = 4 * 1024 * 1024;
	/**
	 * How much may wait in the data channels before the session is given up on. Only a peer that
	 * stopped reading gets here, and without a cap every packet sent to it stays in memory.
	 */
	private const MAX_QUEUED_BYTES = 64 * 1024 * 1024;

	/** How much of a packet fits in one message, once the counter byte is taken off the peer's limit */
	private int $maxMessagePayload = self::MAX_MESSAGE_SIZE;

	private bool $connected = true;
	/** Where the media really flows, once ICE has picked a pair. Until then, where the join came from */
	private string $remoteAddress;
	private int $remotePort;
	private bool $openNotified = false;
	private ?string $authenticatedPublicKey = null;
	private ?RtcChannel $reliableChannel = null;
	private ?RtcChannel $unreliableChannel = null;
	private MessageAssembler $reliableAssembler;
	private MessageAssembler $unreliableAssembler;
	/** @var string[] */
	private array $pendingPackets = [];

	private int $bytesSent = 0;
	private int $bytesReceived = 0;
	private int $createdAt;
	private int $lastReceiveAt;
	/** What the connection had received the last time anyone looked, game data or not */
	private int $lastTransportBytes = 0;
	private int $lastTransportActivityAt;

	/** @var \Closure(string) : void */
	private \Closure $packetHandler;
	/** @var \Closure(string) : void */
	private \Closure $closeHandler;
	/** @var \Closure(int) : void */
	private \Closure $ackHandler;

	public function __construct(
		private RtcPeer $peer,
		private int $connectionId,
		private string $address,
		private int $port,
		\Closure $packetHandler,
		\Closure $closeHandler,
		\Closure $ackHandler
	){
		$this->packetHandler = $packetHandler;
		$this->closeHandler = $closeHandler;
		$this->ackHandler = $ackHandler;
		$this->remoteAddress = $address;
		$this->remotePort = $port;
		$this->createdAt = time();
		$this->lastReceiveAt = $this->createdAt;
		$this->lastTransportActivityAt = $this->createdAt;
		$this->reliableAssembler = new MessageAssembler(true, AnswerRewriter::MAX_MESSAGE_SIZE, self::MAX_PACKET_SIZE);
		$this->unreliableAssembler = new MessageAssembler(false, AnswerRewriter::MAX_MESSAGE_SIZE, self::MAX_PACKET_SIZE);
	}

	/**
	 * Keeps every message within what the peer said it accepts. A peer that set no limit at all
	 * still gets nothing larger than this side advertises for itself.
	 */
	public function limitMessageSize(int $peerMaxMessageSize) : void{
		if($peerMaxMessageSize > 1){
			$this->maxMessagePayload = min(self::MAX_MESSAGE_SIZE, $peerMaxMessageSize - 1);
		}
	}

	public function getId() : int{
		return $this->connectionId;
	}

	/**
	 * The peer's address on the candidate pair ICE selected, or the one its signalling came from
	 * while there is no pair yet.
	 */
	public function getAddress() : string{
		return $this->remoteAddress;
	}

	public function getPort() : int{
		return $this->remotePort;
	}

	/**
	 * Where the join was signalled from: the discovery datagram's source, or the HTTP client for an
	 * endpoint join. Only that address may speak for the connection on the signalling side.
	 */
	public function getSignallingAddress() : string{
		return $this->address;
	}

	public function getSignallingPort() : int{
		return $this->port;
	}

	public function setRemoteAddress(string $address, int $port) : void{
		$this->remoteAddress = $address;
		$this->remotePort = $port;
	}

	public function getPing() : int{
		$rtt = $this->peer->getRtt();
		if($rtt === null){
			return -1;
		}
		//that's a full round trip, ping is conventionally one-way
		return intdiv($rtt, 2);
	}

	public function isConnected() : bool{
		return $this->connected;
	}

	public function collectBandwidthDelta() : array{
		$delta = [$this->bytesSent, $this->bytesReceived];
		$this->bytesSent = 0;
		$this->bytesReceived = 0;
		return $delta;
	}

	/**
	 * Which of the two channels a data channel with these properties would be, or null when it's neither: a channel
	 * the protocol doesn't define, or one under the right name that doesn't deliver the way that name promises.
	 */
	public static function classifyChannel(string $label, string $protocol, bool $ordered, ?int $maxRetransmits, ?int $maxPacketLifeTime) : ?string{
		if($protocol !== "" || $maxPacketLifeTime !== null){
			return null;
		}
		//segments are only put back together in the order they were sent, so nothing may be lost or reordered
		if($label === self::RELIABLE_CHANNEL && $ordered && $maxRetransmits === null){
			return self::RELIABLE_CHANNEL;
		}
		if($label === self::UNRELIABLE_CHANNEL && !$ordered && $maxRetransmits === 0){
			return self::UNRELIABLE_CHANNEL;
		}
		return null;
	}

	/**
	 * Binds a data channel to the session. Returns false for a channel classifyChannel() doesn't accept, or one
	 * already bound.
	 */
	public function bindChannel(RtcChannel $channel) : bool{
		$kind = self::classifyChannel($channel->getLabel(), $channel->getProtocol(), $channel->isOrdered(), $channel->getMaxRetransmits(), $channel->getMaxPacketLifeTime());
		if($kind === self::RELIABLE_CHANNEL && $this->reliableChannel === null){
			$this->reliableChannel = $channel;
			$channel->onMessage(function(string $data) : void{
				$this->handleMessage($data, $this->reliableAssembler);
			});
			//losing the reliable channel means losing the session; the unreliable one carries
			//nothing the game depends on, so its closure is handled by the connection state instead
			$channel->onClose(function() : void{
				$this->onClosed();
			});
			return true;
		}
		if($kind === self::UNRELIABLE_CHANNEL && $this->unreliableChannel === null){
			$this->unreliableChannel = $channel;
			$channel->onMessage(function(string $data) : void{
				$this->handleMessage($data, $this->unreliableAssembler);
			});
			return true;
		}
		return false;
	}

	/**
	 * A session is only usable once the peer has opened both of its data channels - vanilla always
	 * opens the pair, and reporting the session before the unreliable one exists would hand out a
	 * half-negotiated connection.
	 */
	public function isReady() : bool{
		return self::isChannelOpen($this->reliableChannel) && self::isChannelOpen($this->unreliableChannel);
	}

	private static function isChannelOpen(?RtcChannel $channel) : bool{
		return $channel !== null && $channel->isOpen();
	}

	public function getCreatedAt() : int{
		return $this->createdAt;
	}

	/**
	 * When this session last had anything to say. A peer that goes away without closing - a crashed
	 * client, a laptop lid, a router - leaves its connection looking perfectly healthy from here,
	 * and nothing below this layer will ever tell us otherwise.
	 *
	 * SCTP packets count as well as game data. A console can go well past half a minute loading the
	 * world without sending a single game packet, but it keeps acking what we stream at it.
	 */
	public function getLastReceiveAt() : int{
		//anything at all arriving on the connection counts, so this has to be asked often enough to notice
		$received = $this->peer->getBytesReceived();
		if($received !== $this->lastTransportBytes){
			$this->lastTransportBytes = $received;
			$this->lastTransportActivityAt = time();
		}
		return max($this->lastReceiveAt, $this->lastTransportActivityAt);
	}

	public function isOpenNotified() : bool{
		return $this->openNotified;
	}

	public function setAuthenticatedPublicKey(?string $publicKeyBase64) : void{
		$this->authenticatedPublicKey = $publicKeyBase64;
	}

	public function getAuthenticatedPublicKey() : ?string{
		return $this->authenticatedPublicKey;
	}

	public function markOpenNotified() : bool{
		if($this->openNotified){
			return false;
		}
		return $this->openNotified = true;
	}

	public function getPeer() : RtcPeer{
		return $this->peer;
	}

	private function handleMessage(string $data, MessageAssembler $assembler) : void{
		if(!$this->connected){
			return;
		}
		$this->bytesReceived += strlen($data);
		$this->lastReceiveAt = time();
		try{
			$packet = $assembler->push($data);
		}catch(MessageFormatException $e){
			$this->closeWithError($e->getMessage());
			return;
		}
		if($packet === null){
			return;
		}
		if(!$this->openNotified){
			if(count($this->pendingPackets) >= self::MAX_PENDING_PACKETS){
				$this->closeWithError("received more than " . self::MAX_PENDING_PACKETS . " packets before the session was ready");
				return;
			}
			$this->pendingPackets[] = $packet;
			return;
		}
		($this->packetHandler)($packet);
	}

	public function flushPendingPackets() : void{
		$packets = $this->pendingPackets;
		$this->pendingPackets = [];
		foreach($packets as $packet){
			($this->packetHandler)($packet);
		}
	}

	/**
	 * Reports why the session is going away before tearing the connection down. Closing the peer
	 * connection first would fire its own state change, and that reaches the transport as a plain
	 * "the connection closed" - which is true, but says nothing about what actually happened.
	 */
	private function closeWithError(string $reason) : void{
		if(!$this->connected){
			return;
		}
		($this->closeHandler)($reason);
		$this->disconnect();
	}

	public function sendPacket(string $payload, bool $immediate = false, ?int $receiptId = null) : void{
		//a session exists from the moment its connection does, but the channel only carries anything
		//once the peer has opened it - and it stops carrying the moment the peer goes away
		if(!$this->connected || !self::isChannelOpen($this->reliableChannel)){
			return;
		}
		$length = strlen($payload);
		if(!$this->hasRoomFor($length)){
			return;
		}
		$segments = max(1, intdiv($length + $this->maxMessagePayload - 1, $this->maxMessagePayload));
		if($segments > self::MAX_SEGMENTS){
			throw new \InvalidArgumentException("Payload of $length bytes requires $segments segments (max " . self::MAX_SEGMENTS . ")");
		}
		$remaining = $segments - 1;
		for($offset = 0; $offset === 0 || $offset < $length; $offset += $this->maxMessagePayload){
			$this->sendSegment($this->reliableChannel, $remaining, substr($payload, $offset, $this->maxMessagePayload));
			$remaining--;
		}
		if($receiptId !== null){
			($this->ackHandler)($receiptId);
		}
	}

	/**
	 * Sends a packet over the unreliable channel, which the game itself does not currently use.
	 *
	 * The payload cannot be split: a dropped segment would strand the rest, so anything larger than
	 * a single message is rejected rather than sent in a form the peer could never reassemble.
	 */
	public function sendUnreliablePacket(string $payload) : void{
		if(!$this->connected || !self::isChannelOpen($this->unreliableChannel)){
			return;
		}
		$length = strlen($payload);
		if($length > $this->maxMessagePayload){
			throw new \InvalidArgumentException("Payload of $length bytes exceeds the $this->maxMessagePayload byte limit of the unreliable channel");
		}
		if(!$this->hasRoomFor($length)){
			return;
		}
		$this->sendSegment($this->unreliableChannel, 0, $payload);
	}

	/**
	 * What the data channels hold that has yet to go out on the wire.
	 */
	public function getQueuedBytes() : int{
		return ($this->reliableChannel?->getBufferedAmount() ?? 0) + ($this->unreliableChannel?->getBufferedAmount() ?? 0);
	}

	/**
	 * Closes the session when queueing $length more bytes would pass the cap.
	 */
	private function hasRoomFor(int $length) : bool{
		if($this->getQueuedBytes() + $length <= self::MAX_QUEUED_BYTES){
			return true;
		}
		$this->closeWithError("more than " . self::MAX_QUEUED_BYTES . " bytes are waiting to be sent");
		return false;
	}

	private function sendSegment(RtcChannel $channel, int $remaining, string $payload) : void{
		$segment = chr($remaining) . $payload;
		try{
			$channel->send($segment);
		}catch(\RuntimeException $e){
			//the channel can close between the check above and the write, and a peer disappearing
			//mid-send must not take the caller with it
			$this->closeWithError("data channel refused a write: " . $e->getMessage());

			return;
		}
		$this->bytesSent += strlen($segment);
	}

	public function disconnect() : void{
		if($this->connected){
			$this->connected = false;
			$this->peer->close();
		}
	}

	private function onClosed() : void{
		if($this->connected){
			$this->closeWithError("data channel closed by the remote peer");
		}
	}
}
