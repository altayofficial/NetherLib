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

use altay\datachannel\Context;
use altay\datachannel\DataChannel;
use function count;

/**
 * Turns the flat event queue ext-datachannel hands out into calls on the peer and channel they belong to. Everything
 * here runs on the thread that owns the server, and only when poll() is called.
 *
 * @internal
 */
final class RtcContext{
	/**
	 * Channel events that showed up before the channel did. The extension attaches a remote channel's callbacks
	 * before it reports the channel, so an open or a first message can land just ahead of it in the queue.
	 */
	private const MAX_EARLY_EVENTS = 64;

	private Context $context;
	/** @var RtcPeer[] */
	private array $peers = [];
	/** @var RtcChannel[] */
	private array $channels = [];
	/**
	 * @var mixed[][][]
	 * @phpstan-var array<int, list<array{int, int, int, mixed, mixed}>>
	 */
	private array $earlyEvents = [];

	public function __construct(
		private \Logger $logger
	){
		$this->context = new Context();
	}

	public function setVerbose(bool $verbose) : void{
		Context::setLogLevel($verbose ? Context::LOG_DEBUG : Context::LOG_WARNING);
	}

	/**
	 * @param mixed[] $options
	 * @phpstan-param array<string, mixed> $options
	 */
	public function createPeer(array $options) : RtcPeer{
		$peer = new RtcPeer($this->context->createPeerConnection($options), $this);
		$this->peers[$peer->getId()] = $peer;
		return $peer;
	}

	public function track(RtcChannel $channel) : void{
		$this->channels[$channel->getId()] = $channel;
	}

	public function forget(RtcPeer $peer) : void{
		unset($this->peers[$peer->getId()]);
		foreach($peer->getChannels() as $channel){
			unset($this->channels[$channel->getId()], $this->earlyEvents[$channel->getId()]);
		}
	}

	public function getPeerCount() : int{
		return count($this->peers);
	}

	public function poll() : void{
		foreach($this->context->poll() as $event){
			try{
				$this->dispatch($event);
			}catch(\Throwable $e){
				//one peer's handler blowing up must not take every other connection down with it
				$this->logger->logException($e);
			}
		}
	}

	/**
	 * @phpstan-param array{int, int, int, mixed, mixed} $event
	 */
	private function dispatch(array $event) : void{
		[$type, $peerId, $channelId, $value, $extra] = $event;
		switch($type){
			case Context::EVENT_LOG:
				$level = (int) $extra;
				$line = "libdatachannel: " . (string) $value;
				if($level <= Context::LOG_ERROR){
					$this->logger->error($line);
				}elseif($level === Context::LOG_WARNING){
					$this->logger->warning($line);
				}else{
					$this->logger->debug($line);
				}
				return;
			case Context::EVENT_DATA_CHANNEL:
				$this->handleNewChannel($peerId, $channelId, $value);
				return;
			case Context::EVENT_CHANNEL_OPEN:
			case Context::EVENT_CHANNEL_CLOSED:
			case Context::EVENT_CHANNEL_ERROR:
			case Context::EVENT_MESSAGE:
			case Context::EVENT_BUFFERED_AMOUNT_LOW:
				$channel = $this->channels[$channelId] ?? null;
				if($channel !== null){
					$channel->dispatch($type, $value);
				}elseif(isset($this->peers[$peerId]) && count($this->earlyEvents[$channelId] ?? []) < self::MAX_EARLY_EVENTS){
					$this->earlyEvents[$channelId][] = $event;
				}
				return;
		}

		$peer = $this->peers[$peerId] ?? null;
		if($peer === null){
			return;
		}
		switch($type){
			case Context::EVENT_STATE:
				$peer->dispatchState((int) $value);
				break;
			case Context::EVENT_GATHERING_STATE:
				$peer->dispatchGatheringState((int) $value);
				break;
			case Context::EVENT_LOCAL_DESCRIPTION:
				$peer->dispatchLocalDescription((string) $value, (string) $extra);
				break;
			case Context::EVENT_LOCAL_CANDIDATE:
				$peer->dispatchLocalCandidate((string) $value);
				break;
		}
	}

	private function handleNewChannel(int $peerId, int $channelId, mixed $value) : void{
		if(!$value instanceof DataChannel){
			return;
		}
		$peer = $this->peers[$peerId] ?? null;
		if($peer === null){
			//its connection is already gone from our side
			$value->close();
			return;
		}
		$channel = new RtcChannel($value);
		$this->track($channel);
		$peer->dispatchDataChannel($channel);

		$early = $this->earlyEvents[$channelId] ?? [];
		unset($this->earlyEvents[$channelId]);
		foreach($early as $event){
			$this->dispatch($event);
		}
	}
}
