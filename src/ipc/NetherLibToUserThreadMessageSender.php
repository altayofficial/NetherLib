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

namespace altay\netherlib\ipc;

use altay\netherlib\ServerListener;
use pocketmine\utils\Binary;

final class NetherLibToUserThreadMessageSender implements ServerListener{

	public function __construct(
		private InterThreadChannelWriter $channel
	){}

	public function onSessionOpen(int $sessionId, string $address, int $port, ?string $authenticatedPublicKey) : void{
		$publicKey = $authenticatedPublicKey ?? "";
		$this->channel->write(
			chr(EventId::SESSION_OPEN) .
			Binary::writeLLong($sessionId) .
			chr(strlen($address)) . $address .
			Binary::writeLShort($port) .
			Binary::writeLShort(strlen($publicKey)) . $publicKey
		);
	}

	public function onSessionClose(int $sessionId, string $reason) : void{
		$this->channel->write(
			chr(EventId::SESSION_CLOSE) .
			Binary::writeLLong($sessionId) .
			$reason
		);
	}

	public function onPacketReceive(int $sessionId, string $payload) : void{
		$this->channel->write(
			chr(EventId::PACKET_RECEIVE) .
			Binary::writeLLong($sessionId) .
			$payload
		);
	}

	public function onPacketAck(int $sessionId, int $receiptId) : void{
		$this->channel->write(
			chr(EventId::PACKET_ACK) .
			Binary::writeLLong($sessionId) .
			Binary::writeLLong($receiptId)
		);
	}

	public function onPingUpdate(int $sessionId, int $pingMS) : void{
		$this->channel->write(
			chr(EventId::PING_UPDATE) .
			Binary::writeLLong($sessionId) .
			Binary::writeLInt($pingMS)
		);
	}

	public function onBandwidthUpdate(int $bytesSentDiff, int $bytesReceivedDiff) : void{
		$this->channel->write(
			chr(EventId::BANDWIDTH_UPDATE) .
			Binary::writeLLong($bytesSentDiff) .
			Binary::writeLLong($bytesReceivedDiff)
		);
	}
}
