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
use altay\datachannel\DataChannelException;

/**
 * @internal
 */
class RtcChannel{
	/** @var list<\Closure() : void> */
	private array $openListeners = [];
	/** @var list<\Closure() : void> */
	private array $closeListeners = [];
	/** @var list<\Closure(string) : void> */
	private array $messageListeners = [];

	public function __construct(
		private DataChannel $channel
	){}

	public function getId() : int{
		return $this->channel->getId();
	}

	public function getLabel() : string{
		return $this->channel->getLabel();
	}

	public function getProtocol() : string{
		return $this->channel->getProtocol();
	}

	public function isOrdered() : bool{
		return $this->channel->isOrdered();
	}

	public function getMaxRetransmits() : ?int{
		return $this->channel->getMaxRetransmits();
	}

	public function getMaxPacketLifeTime() : ?int{
		return $this->channel->getMaxPacketLifeTime();
	}

	public function isOpen() : bool{
		return $this->channel->isOpen();
	}

	public function getBufferedAmount() : int{
		return $this->channel->getBufferedAmount();
	}

	/**
	 * @throws DataChannelException
	 */
	public function send(string $data) : void{
		//false only means the library had to buffer it, which getBufferedAmount() keeps track of
		$this->channel->send($data);
	}

	/** @param \Closure() : void $listener */
	public function onOpen(\Closure $listener) : void{
		$this->openListeners[] = $listener;
	}

	/** @param \Closure() : void $listener */
	public function onClose(\Closure $listener) : void{
		$this->closeListeners[] = $listener;
	}

	/** @param \Closure(string) : void $listener */
	public function onMessage(\Closure $listener) : void{
		$this->messageListeners[] = $listener;
	}

	public function close() : void{
		try{
			$this->channel->close();
		}catch(DataChannelException){

		}
	}

	/**
	 * Drops the listeners, so whatever they close over can be freed once the connection is gone.
	 */
	public function release() : void{
		$this->openListeners = [];
		$this->closeListeners = [];
		$this->messageListeners = [];
	}

	public function dispatch(int $type, mixed $value) : void{
		switch($type){
			case Context::EVENT_CHANNEL_OPEN:
				foreach($this->openListeners as $listener){
					$listener();
				}
				break;
			case Context::EVENT_CHANNEL_CLOSED:
				foreach($this->closeListeners as $listener){
					$listener();
				}
				break;
			case Context::EVENT_MESSAGE:
				foreach($this->messageListeners as $listener){
					$listener((string) $value);
				}
				break;
		}
	}
}
