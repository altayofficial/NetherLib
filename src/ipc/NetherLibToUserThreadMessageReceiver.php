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

final class NetherLibToUserThreadMessageReceiver{

	public function __construct(
		private InterThreadChannelReader $channel
	){}

	public function handle(ServerListener $listener) : bool{
		$message = $this->channel->read();
		if($message === null){
			return false;
		}

		$id = ord($message[0]);
		$offset = 1;
		switch($id){
			case EventId::SESSION_OPEN:
				$sessionId = Binary::readLLong(substr($message, $offset, 8));
				$offset += 8;
				$addressLength = ord($message[$offset++]);
				$address = substr($message, $offset, $addressLength);
				$offset += $addressLength;
				$port = Binary::readLShort(substr($message, $offset, 2));
				$offset += 2;
				$publicKeyLength = Binary::readLShort(substr($message, $offset, 2));
				$offset += 2;
				$publicKey = $publicKeyLength > 0 ? substr($message, $offset, $publicKeyLength) : null;
				$listener->onSessionOpen($sessionId, $address, $port, $publicKey);
				break;
			case EventId::SESSION_CLOSE:
				$sessionId = Binary::readLLong(substr($message, $offset, 8));
				$listener->onSessionClose($sessionId, substr($message, $offset + 8));
				break;
			case EventId::PACKET_RECEIVE:
				$sessionId = Binary::readLLong(substr($message, $offset, 8));
				$listener->onPacketReceive($sessionId, substr($message, $offset + 8));
				break;
			case EventId::PACKET_ACK:
				$sessionId = Binary::readLLong(substr($message, $offset, 8));
				$listener->onPacketAck($sessionId, Binary::readLLong(substr($message, $offset + 8, 8)));
				break;
			case EventId::PING_UPDATE:
				$sessionId = Binary::readLLong(substr($message, $offset, 8));
				$listener->onPingUpdate($sessionId, Binary::readLInt(substr($message, $offset + 8, 4)));
				break;
			case EventId::RAW_PACKET_RECEIVE:
				$addressLength = ord($message[$offset++]);
				$address = substr($message, $offset, $addressLength);
				$offset += $addressLength;
				$port = Binary::readLShort(substr($message, $offset, 2));
				$listener->onRawPacketReceive($address, $port, substr($message, $offset + 2));
				break;
			case EventId::BANDWIDTH_UPDATE:
				$sent = Binary::readLLong(substr($message, $offset, 8));
				$received = Binary::readLLong(substr($message, $offset + 8, 8));
				$listener->onBandwidthUpdate($sent, $received);
				break;
		}
		return true;
	}
}
