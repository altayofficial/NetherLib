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

namespace altay\netherlib\rtc;

use altay\datachannel\DataChannelException;
use altay\datachannel\PeerConnection;

/**
 * One peer connection and whoever is listening to it.
 *
 * @internal
 */
class RtcPeer{
	/** @var list<\Closure(int) : void> */
	private array $stateListeners = [];
	/** @var list<\Closure() : void> */
	private array $gatheringCompleteListeners = [];
	/** @var list<\Closure(string, string) : void> */
	private array $localDescriptionListeners = [];
	/** @var list<\Closure(string) : void> */
	private array $localCandidateListeners = [];
	/** @var list<\Closure(RtcChannel) : void> */
	private array $dataChannelListeners = [];

	/** @var list<string> */
	private array $localCandidates = [];
	private bool $gatheringComplete = false;
	/** @var RtcChannel[] */
	private array $channels = [];
	private bool $closed = false;

	public function __construct(
		private PeerConnection $connection,
		private RtcContext $context
	){}

	public function getId() : int{
		return $this->connection->getId();
	}

	/** @param \Closure(int) : void $listener */
	public function onStateChange(\Closure $listener) : void{
		$this->stateListeners[] = $listener;
	}

	/** @param \Closure() : void $listener */
	public function onGatheringComplete(\Closure $listener) : void{
		$this->gatheringCompleteListeners[] = $listener;
	}

	/** @param \Closure(string $sdp, string $type) : void $listener */
	public function onLocalDescription(\Closure $listener) : void{
		$this->localDescriptionListeners[] = $listener;
	}

	/** @param \Closure(string $candidate) : void $listener */
	public function onLocalCandidate(\Closure $listener) : void{
		$this->localCandidateListeners[] = $listener;
	}

	/** @param \Closure(RtcChannel) : void $listener */
	public function onDataChannel(\Closure $listener) : void{
		$this->dataChannelListeners[] = $listener;
	}

	/**
	 * @throws DataChannelException
	 */
	public function setRemoteDescription(string $sdp, string $type) : void{
		$this->connection->setRemoteDescription($sdp, $type);
	}

	/**
	 * @throws DataChannelException
	 */
	public function addRemoteCandidate(string $candidate) : void{
		//NetherNet only ever negotiates one media section
		$this->connection->addRemoteCandidate($candidate, "0");
	}

	/**
	 * @param mixed[] $options
	 * @phpstan-param array<string, mixed> $options
	 * @throws DataChannelException
	 */
	public function createDataChannel(string $label, array $options = []) : RtcChannel{
		$channel = new RtcChannel($this->connection->createDataChannel($label, $options));
		$this->channels[] = $channel;
		$this->context->track($channel);
		return $channel;
	}

	/**
	 * @return RtcChannel[]
	 */
	public function getChannels() : array{
		return $this->channels;
	}

	/**
	 * The local description, with every candidate gathered so far in it.
	 */
	public function getLocalDescription() : ?string{
		return $this->connection->getLocalDescription();
	}

	/**
	 * @return list<string> candidate lines, "candidate:..." without the "a="
	 */
	public function getLocalCandidates() : array{
		return $this->localCandidates;
	}

	public function isGatheringComplete() : bool{
		return $this->gatheringComplete;
	}

	public function getState() : int{
		return $this->connection->getState();
	}

	public function getCloseReason() : int{
		return $this->connection->getCloseReason();
	}

	/**
	 * Round trip time in milliseconds, or null until one was measured.
	 */
	public function getRtt() : ?int{
		return $this->connection->getRtt();
	}

	public function getBytesReceived() : int{
		return $this->connection->getBytesReceived();
	}

	/**
	 * @return array{local: array{string, int, string}, remote: array{string, int, string}}|null
	 */
	public function getSelectedCandidatePair() : ?array{
		return $this->connection->getSelectedCandidatePair();
	}

	public function isClosed() : bool{
		return $this->closed;
	}

	public function close() : void{
		if($this->closed){
			return;
		}
		$this->closed = true;
		try{
			$this->connection->close();
		}catch(DataChannelException){
			//already on its way down
		}
		$this->context->forget($this);
		$this->stateListeners = [];
		$this->gatheringCompleteListeners = [];
		$this->localDescriptionListeners = [];
		$this->localCandidateListeners = [];
		$this->dataChannelListeners = [];
		foreach($this->channels as $channel){
			$channel->release();
		}
	}

	public function dispatchState(int $state) : void{
		foreach($this->stateListeners as $listener){
			$listener($state);
		}
	}

	public function dispatchGatheringState(int $state) : void{
		if($state !== PeerConnection::GATHERING_COMPLETE || $this->gatheringComplete){
			return;
		}
		$this->gatheringComplete = true;
		foreach($this->gatheringCompleteListeners as $listener){
			$listener();
		}
	}

	public function dispatchLocalDescription(string $sdp, string $type) : void{
		foreach($this->localDescriptionListeners as $listener){
			$listener($sdp, $type);
		}
	}

	public function dispatchLocalCandidate(string $candidate) : void{
		$this->localCandidates[] = $candidate;
		foreach($this->localCandidateListeners as $listener){
			$listener($candidate);
		}
	}

	public function dispatchDataChannel(RtcChannel $channel) : void{
		$this->channels[] = $channel;
		if($this->dataChannelListeners === []){
			$channel->close();
			return;
		}
		foreach($this->dataChannelListeners as $listener){
			$listener($channel);
		}
	}
}
